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
) -> str:
    """Write rows as all-STRING parquet, hive-partitioned where applicable.

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
    if os.path.isdir(target):
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


def build_all(
    years: Iterable[int] | None = None,
    extracted_at: str | None = None,
    today: dt.date | None = None,
    sub_resources: bool = True,
) -> dict[str, list[dict]]:
    """Run every builder and return the full table set, keyed by table slug.

    ``years`` bounds the four genuine time series; ``None`` means full history
    from each source's own first year. ``sub_resources=False`` skips the
    contratação fan-out (itens, garantias, pagamentos and their children), which
    is the slow half of the extract and refreshes on its own weekly schedule.
    """
    today = today or dt.date.today()
    extracted_at = extracted_at or today.isoformat()
    out: dict[str, list[dict]] = {}

    def span(first: int) -> list[int]:
        return (
            list(years)
            if years is not None
            else list(range(first, today.year + 1))
        )

    # --- Senadores
    out["despesa_ceaps"] = clean.build_despesa_ceaps(span(FIRST_YEAR_CEAPS))
    out["senador_gabinete"] = clean.build_senador_gabinete(extracted_at)
    out["senador_escritorio_apoio"] = clean.build_senador_escritorio_apoio(
        extracted_at
    )
    out["senador_auxilio_moradia"] = clean.build_senador_auxilio_moradia(
        extracted_at
    )
    out["senador_aposentado_pensionista"] = (
        clean.build_senador_aposentado_pensionista(extracted_at)
    )

    # --- Servidores
    out["servidor"] = clean.build_servidor(extracted_at)
    out["servidor_ativo"] = clean.build_servidor_ativo(extracted_at)
    out["servidor_remuneracao"] = clean.build_servidor_remuneracao(
        months(span(FIRST_YEAR_REMUNERACAO), today)
    )
    parents, days = clean.build_servidor_hora_extra(
        months(span(FIRST_YEAR_HORA_EXTRA), today)
    )
    out["servidor_hora_extra"] = parents
    out["servidor_hora_extra_dia"] = days
    out["servidor_aposentado"] = clean.build_servidor_aposentado(extracted_at)
    out["servidor_exonerado"] = clean.build_servidor_exonerado(extracted_at)
    out["servidor_cedido"] = clean.build_servidor_cedido(extracted_at)
    out["pensionista"] = clean.build_pensionista(extracted_at)

    # --- Contratações. One fan-out over the status enum feeds every child.
    raw = api.fetch_contratacoes()
    out["contratacao"] = clean.build_contratacao(raw, extracted_at)
    out["contratacao_orgao_gestor"] = clean.build_contratacao_orgao_gestor(
        raw, extracted_at
    )
    licitacoes, detalhamentos = clean.build_licitacao(extracted_at)
    out["licitacao"] = licitacoes
    out["licitacao_detalhamento"] = detalhamentos
    out["empresa"] = clean.build_empresa(extracted_at)

    if sub_resources:
        out["contratacao_item"] = clean.build_contratacao_item(
            raw, extracted_at
        )
        out["contratacao_garantia"] = clean.build_contratacao_garantia(
            raw, extracted_at
        )
        pagamentos, documentos, empenhos = clean.build_contratacao_pagamento(
            raw, extracted_at
        )
        out["contratacao_pagamento"] = pagamentos
        out["contratacao_documento_fiscal"] = documentos
        out["contratacao_pagamento_empenho"] = empenhos
        out["contrato_aditivo"] = clean.build_contrato_aditivo(
            raw, extracted_at
        )
        out["ata_acionamento"] = clean.build_ata_acionamento(raw, extracted_at)

    # --- Colaboradores
    out["terceirizado"] = clean.build_terceirizado(extracted_at)
    out["menor_aprendiz"] = clean.build_menor_aprendiz(extracted_at)
    out["estagiario"] = clean.build_estagiario(extracted_at)

    # --- Supridos: one request per year yields all six tables
    out.update(clean.build_supridos(span(FIRST_YEAR_SUPRIDOS)))

    # --- Gestão
    out["quadro_pessoal"] = clean.build_quadro_pessoal(extracted_at)
    out["diretor_coordenador"] = clean.build_diretor_coordenador(extracted_at)
    out["previsao_aposentadoria"] = clean.build_previsao_aposentadoria(
        extracted_at
    )

    out["dicionario"] = clean.build_dicionario(out)
    return out


def clean_all(
    output: str,
    years: Iterable[int] | None = None,
    extracted_at: str | None = None,
    today: dt.date | None = None,
    sub_resources: bool = True,
) -> dict[str, str]:
    """Build every table and write it as all-STRING partitioned parquet.

    Returns the output directory per table.
    """
    tables = build_all(
        years=years,
        extracted_at=extracted_at,
        today=today,
        sub_resources=sub_resources,
    )
    written = {}
    for table, rows in tables.items():
        written[table] = write_partitioned(
            rows, output, table, TABLES[table]["partition"]
        )
    return written
