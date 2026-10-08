"""Prefect 3 tasks for br_mf_divida_ativa — thin wrappers over utils.py.

As 3 tabelas (nao_previdenciario / previdenciario / fgts) — ver constants.py.

PGFN publica as 3 juntas a cada trimestre, mas em um ZIP por tabela — o
download É decomponível por tabela (``download_quarter(table=...)``). O que
NÃO é decomponível é a sonda de "qual é o trimestre mais novo publicado": só a
SIDA (nao_previdenciario) é garantida presente em todo trimestre (ver
docstring de ``latest_available_quarter``, e o antigo ``ANCHOR_TABLE`` do flow
monolítico) — por isso ``get_latest_update`` é uma função só, compartilhada
pelas 3 tabelas via ``lambda _: get_latest_update`` em ``make_pipeline``.

``extract_load_data(table_id)`` já é por tabela: cada chamada relê a própria
Coverage já registrada da SUA tabela (``task_get_api_most_recent_date``) e
baixa todo trimestre mais novo que ela — não só o mais recente — igual ao
catch-up de br_bcb_agencia/br_bcb_estban (mais de um trimestre de atraso é
baixado de uma vez na mesma chamada).
"""

import tempfile
from collections.abc import Callable
from datetime import date
from pathlib import Path

from pipelines.datasets.br_mf_divida_ativa.constants import COVERAGE, constants
from pipelines.datasets.br_mf_divida_ativa.utils import (
    FIRST_QUARTER,
    FIRST_YEAR,
    all_quarters,
    clean_quarters,
    latest_available_quarter,
    quarter_date_str,
)
from pipelines.utils.metadata.tasks import task_get_api_most_recent_date
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)

DATASET_ID = constants.DATASET_ID.value


def get_latest_update() -> SourceInspection:
    """Sonda a fonte (via SIDA) pelo trimestre mais novo publicado.

    Compartilhada pelas 3 tabelas — só a SIDA garante presença em todo
    trimestre, então é a única sonda confiável pra decidir "existe trimestre
    novo publicado" (ver ``latest_available_quarter``).

    Returns:
        ``SourceInspection`` com ``reference_date`` no primeiro dia do último
        mês do trimestre mais novo (ex. 2026 Q2 -> 2026-06-01).
        ``compare_against="coverage"`` — igual ao flow monolítico antigo —
        cada tabela compara essa data contra a SUA PRÓPRIA Coverage.

    Raises:
        RuntimeError: a fonte está inacessível (nem o primeiro trimestre
            responde).
    """
    available = latest_available_quarter()
    if available is None:
        raise RuntimeError(
            "PGFN source unreachable — could not find even the first quarter."
        )
    return SourceInspection(
        reference_date=date.fromisoformat(quarter_date_str(*available)),
        compare_against="coverage",
    )


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    """Fábrica do extract_and_load de uma tabela — catch-up por Coverage própria."""

    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        reference_date = date.fromisoformat(download_params["reference_date"])
        available = (reference_date.year, reference_date.month // 3)

        # Catch-up por tabela: relê a Coverage já registrada desta tabela (não
        # a sonda compartilhada acima), pra baixar TODO trimestre mais novo
        # que ela, não só o mais recente — uma tabela que ficou pra trás (ex.
        # uma falha isolada num run anterior) se recupera sozinha.
        api_max_date = task_get_api_most_recent_date(
            dataset_id=DATASET_ID, table_id=table_id, date_format="%Y-%m"
        )
        candidates = all_quarters((FIRST_YEAR, FIRST_QUARTER), available)
        new_quarters = (
            candidates
            if api_max_date is None
            else [
                (y, q)
                for (y, q) in candidates
                if date(y, q * 3, 1) > api_max_date
            ]
        )

        work_dir = tempfile.mkdtemp(prefix=f"br_mf_divida_ativa_{table_id}_")
        data_path = clean_quarters(table_id, new_quarters, Path(work_dir))
        if data_path is None:
            raise RuntimeError(
                f"{table_id}: source has none of {new_quarters} "
                "(missing/unpublished for this table)."
            )

        return ExtractAndLoad(
            coverage=COVERAGE[table_id].model_dump(),
            data_path=data_path,
            dump_mode="append",
            source_format="parquet",
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    # Checagem única compartilhada pelas 3 tabelas — ver docstring do módulo.
    lambda _table_id: get_latest_update,
    make_extract_load_data,
    # Mesmo date_format do flow monolítico antigo (poll/commit do boundary).
    date_format="%Y-%m-%d",
)
