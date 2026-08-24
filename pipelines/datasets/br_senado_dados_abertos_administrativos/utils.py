"""
Senado Federal Administrative Open Data — table registry and orchestration.

Pure functions (no Prefect). ``clean_all`` runs the whole extract and writes
all-STRING partitioned parquet, and is shared by the one-shot onboarding under
``models/br_senado_dados_abertos_administrativos/code/`` and by the recurring
pipeline, so both produce byte-identical output.

Column order comes from ``architecture_spec.py`` under ``models/``, which stays
the single source of truth; nothing here restates it.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import os
import shutil
from collections.abc import Iterable
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.datasets.br_senado_dados_abertos_administrativos import (
    senado_adm_api as api,
)
from pipelines.datasets.br_senado_dados_abertos_administrativos import (
    senado_adm_clean as clean,
)

_REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..")
)
ARCHITECTURE = os.path.join(
    _REPO_ROOT,
    "models",
    "br_senado_dados_abertos_administrativos",
    "code",
    "architecture_spec.py",
)

# First period each source exposes; earlier requests answer 404. Probed at the
# boundary, not assumed — see the dataset's ONBOARDING_PLAN.md.
FIRST_YEAR_CEAPS = 2008
FIRST_YEAR_REMUNERACAO = 2013
FIRST_YEAR_HORA_EXTRA = 2013
FIRST_YEAR_SUPRIDOS = 2013


def architecture() -> dict[str, dict]:
    """Load TABLES from the architecture spec under models/."""
    spec = importlib.util.spec_from_file_location(
        "br_senado_adm_architecture", ARCHITECTURE
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load architecture spec at {ARCHITECTURE}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.TABLES


TABLES = architecture()
ALL_TABLES = tuple(TABLES)
PARTITIONED = tuple(
    t for t, meta in TABLES.items() if meta["partition"] == "ano"
)
SNAPSHOTS = tuple(
    t for t, meta in TABLES.items() if meta["partition"] == "data_extracao"
)
UNPARTITIONED = tuple(t for t, meta in TABLES.items() if not meta["partition"])


def months(years: Iterable[int], through: dt.date) -> list[tuple[int, int]]:
    """Every (year, month) in `years` up to and including `through`."""
    out = []
    for year in years:
        last = 12 if year < through.year else through.month
        out.extend((year, month) for month in range(1, last + 1))
    return out


def write_partitioned(
    rows: list[dict],
    out_dir: str,
    table: str,
    partition: str | None,
    reset: bool = True,
) -> str:
    """Write rows as all-STRING parquet, hive-partitioned where applicable.

    ``reset=False`` appends partitions to a table already begun, which is how the
    time series are written a year at a time — see :func:`clean_all`.

    A table with no rows for the requested window writes no files and leaves an
    empty directory — `suprido_movimentacao` is genuinely empty in some years,
    for instance. Callers must treat that as "nothing to upload for this
    window", not as an error: with ``dump_mode="append"`` the partitions already
    in staging stay untouched, which is the correct outcome.

    Staging is all-STRING by house convention and the dbt model safe_casts every
    column, so the schema here carries column *order*, not types. The cast goes
    through arrow rather than ``astype(str)``: the latter renders NULL as the
    literal ``"nan"``, which ``safe_cast`` will not turn back into NULL.
    """
    target = os.path.join(out_dir, table)
    if reset and os.path.isdir(target):
        shutil.rmtree(target)
    os.makedirs(target, exist_ok=True)

    columns = [c[0] for c in TABLES[table]["cols"]]
    value_cols = [c for c in columns if c != partition]
    schema = pa.schema([(c, pa.string()) for c in value_cols])

    def to_string(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    def dump(subset: list[dict], path: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        table_arrow = pa.table(
            {
                col: pa.array(
                    [to_string(r.get(col)) for r in subset], type=pa.string()
                )
                for col in value_cols
            },
            schema=schema,
        )
        pq.write_table(table_arrow, path, compression="snappy")

    if partition is None:
        dump(rows, os.path.join(target, "data.parquet"))
        return target

    buckets: dict[Any, list[dict]] = {}
    for row in rows:
        buckets.setdefault(row.get(partition), []).append(row)
    for key, subset in buckets.items():
        if key is None:
            continue
        dump(
            subset, os.path.join(target, f"{partition}={key}", "data.parquet")
        )
    return target


def _rubricas(rows: list[dict]) -> dict[str, str]:
    """Collect the {rubrica: descrição} pairs present in a supridos table."""
    out: dict[str, str] = {}
    for row in rows:
        code = row.get("rubrica")
        if code:
            out.setdefault(code, row.get("descricao") or "")
    return out


def clean_all(
    output: str,
    years: Iterable[int] | None = None,
    extracted_at: str | None = None,
    today: dt.date | None = None,
    sub_resources: bool = True,
) -> dict[str, int]:
    """Build every table and write it as all-STRING partitioned parquet.

    Tables are written and released one at a time, and the four time series a
    year at a time, so peak memory stays near the largest single year rather
    than the whole extraction. Holding everything first is not an option: as
    Python dicts, servidor_remuneracao and servidor_hora_extra_dia alone come to
    roughly 9 GB over full history, against a 4 GiB worker.

    ``years`` bounds the time series; ``None`` means full history from each
    source's own first year. ``sub_resources=False`` skips the contratação
    fan-out — the slow half of the extract, which refreshes on its own weekly
    schedule.

    Returns the row count written per table.
    """
    today = today or dt.date.today()
    extracted_at = extracted_at or today.isoformat()
    counts: dict[str, int] = {}
    rubricas: dict[str, dict[str, str]] = {}

    def emit(table: str, rows: list[dict], reset: bool = True) -> None:
        write_partitioned(
            rows, output, table, TABLES[table]["partition"], reset=reset
        )
        counts[table] = (0 if reset else counts.get(table, 0)) + len(rows)

    def span(first: int) -> list[int]:
        return (
            list(years)
            if years is not None
            else list(range(first, today.year + 1))
        )

    # --- Senadores
    emit("senador_gabinete", clean.build_senador_gabinete(extracted_at))
    emit(
        "senador_escritorio_apoio",
        clean.build_senador_escritorio_apoio(extracted_at),
    )
    emit(
        "senador_auxilio_moradia",
        clean.build_senador_auxilio_moradia(extracted_at),
    )
    emit(
        "senador_aposentado_pensionista",
        clean.build_senador_aposentado_pensionista(extracted_at),
    )

    # --- Servidores: snapshots
    emit("servidor", clean.build_servidor(extracted_at))
    emit("servidor_ativo", clean.build_servidor_ativo(extracted_at))
    emit("servidor_aposentado", clean.build_servidor_aposentado(extracted_at))
    emit("servidor_exonerado", clean.build_servidor_exonerado(extracted_at))
    emit("servidor_cedido", clean.build_servidor_cedido(extracted_at))
    emit("pensionista", clean.build_pensionista(extracted_at))

    # --- Time series, one year at a time (the memory-critical path)
    for i, year in enumerate(span(FIRST_YEAR_CEAPS)):
        emit("despesa_ceaps", clean.build_despesa_ceaps([year]), reset=i == 0)

    for i, year in enumerate(span(FIRST_YEAR_REMUNERACAO)):
        emit(
            "servidor_remuneracao",
            clean.build_servidor_remuneracao(months([year], today)),
            reset=i == 0,
        )

    for i, year in enumerate(span(FIRST_YEAR_HORA_EXTRA)):
        parents, days = clean.build_servidor_hora_extra(months([year], today))
        emit("servidor_hora_extra", parents, reset=i == 0)
        emit("servidor_hora_extra_dia", days, reset=i == 0)

    for i, year in enumerate(span(FIRST_YEAR_SUPRIDOS)):
        supridos = clean.build_supridos([year])
        for table, rows in supridos.items():
            emit(table, rows, reset=i == 0)
            if table in clean.RUBRICA_TABELAS:
                rubricas.setdefault(table, {}).update(_rubricas(rows))

    # --- Contratações. One status fan-out feeds the parent and every child.
    raw = api.fetch_contratacoes()
    emit("contratacao", clean.build_contratacao(raw, extracted_at))
    emit(
        "contratacao_orgao_gestor",
        clean.build_contratacao_orgao_gestor(raw, extracted_at),
    )
    licitacoes, detalhamentos = clean.build_licitacao(extracted_at)
    emit("licitacao", licitacoes)
    emit("licitacao_detalhamento", detalhamentos)
    emit("empresa", clean.build_empresa(extracted_at))

    if sub_resources:
        emit(
            "contratacao_item", clean.build_contratacao_item(raw, extracted_at)
        )
        emit(
            "contratacao_garantia",
            clean.build_contratacao_garantia(raw, extracted_at),
        )
        pagamentos, documentos, empenhos = clean.build_contratacao_pagamento(
            raw, extracted_at
        )
        emit("contratacao_pagamento", pagamentos)
        emit("contratacao_documento_fiscal", documentos)
        emit("contratacao_pagamento_empenho", empenhos)
        emit(
            "contrato_aditivo", clean.build_contrato_aditivo(raw, extracted_at)
        )
        emit("ata_acionamento", clean.build_ata_acionamento(raw, extracted_at))
    del raw

    # --- Colaboradores
    emit("terceirizado", clean.build_terceirizado(extracted_at))
    emit("menor_aprendiz", clean.build_menor_aprendiz(extracted_at))
    emit("estagiario", clean.build_estagiario(extracted_at))

    # --- Gestão
    emit("quadro_pessoal", clean.build_quadro_pessoal(extracted_at))
    emit("diretor_coordenador", clean.build_diretor_coordenador(extracted_at))
    emit(
        "previsao_aposentadoria",
        clean.build_previsao_aposentadoria(extracted_at),
    )

    emit("dicionario", clean.build_dicionario(rubricas))
    return counts
