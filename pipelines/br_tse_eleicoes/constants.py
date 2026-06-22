"""
Constant values and the table dependency graph for the br_tse_eleicoes pipeline.

This pipeline is the **Prefect 3 orchestration layer only**. The data-cleaning
logic (header-based parsing, normalization, aggregation) lives in
``models/br_tse_eleicoes/code/python`` and is invoked as a subprocess
(see :mod:`pipelines.br_tse_eleicoes.tasks`). The diagnostics harness that
validates that code also lives there.

The dependency graph below is the canonical topological order from
``models/br_tse_eleicoes/code/DEPENDENCIES.md`` (and Track D of the refactor
plan). ``wait_for`` references other ``table_id``s.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path


class constants(Enum):
    """Scalar constants for the br_tse_eleicoes pipeline."""

    DATASET_ID = "br_tse_eleicoes"

    # Root of the data-cleaning code invoked by the build task.
    CODE_DIR = (
        Path(__file__).resolve().parents[2]
        / "models"
        / "br_tse_eleicoes"
        / "code"
        / "python"
    )

    # GCS staging buckets per environment (see bigquery-conventions rule).
    BUCKET = {"dev": "basedosdados-dev", "prod": "basedosdados"}

    # Phase-2 barrier step: builds norm_candidatos + writes the Hive-partitioned
    # parquets every producer table uploads. Runs once, after all phase-1
    # builds and before the phase-3 aggregations / dbt rollups.
    NORMALIZE_STEP = "normalize"
    AGGREGATE_STEP = "aggregate"


# Flow profiles — how each published table is materialized.
PROFILE_PRODUCER = "producer"  # build (python) -> upload_to_gcs -> run_dbt
PROFILE_DBT_ROLLUP = "dbt_rollup"  # run_dbt only (ref() resolves the upstream)
PROFILE_PYTHON_AGG = (
    "python_agg"  # aggregate (python) -> upload_to_gcs -> run_dbt
)


@dataclass(frozen=True)
class TableFlow:
    """One published table and how its flow is wired."""

    table_id: str
    profile: str
    # build.py step in models/code/python that produces this table's parquet
    # (None for pure-dbt rollups). Several tables can share one step.
    build_step: str | None
    wave: int
    # other table_ids whose flow must finish first (topological edges)
    wait_for: tuple[str, ...] = field(default=())


# ---------------------------------------------------------------------------
# Topological table graph (DEPENDENCIES.md → Track D waves)
# ---------------------------------------------------------------------------
TABLE_FLOWS: list[TableFlow] = [
    # Wave 0 — linchpin. Its build also produces norm_candidatos (Phase 2).
    TableFlow("candidatos", PROFILE_PRODUCER, "candidates", 0),
    # Wave 1 — depend on candidatos (título merge in Phase 2)
    TableFlow(
        "bens_candidato",
        PROFILE_PRODUCER,
        "campaign_finance",
        1,
        ("candidatos",),
    ),
    TableFlow(
        "receitas_candidato",
        PROFILE_PRODUCER,
        "campaign_finance",
        1,
        ("candidatos",),
    ),
    TableFlow(
        "despesas_candidato",
        PROFILE_PRODUCER,
        "campaign_finance",
        1,
        ("candidatos",),
    ),
    TableFlow(
        "resultados_candidato_secao",
        PROFILE_PRODUCER,
        "results_section",
        1,
        ("candidatos",),
    ),
    TableFlow(
        "resultados_candidato_municipio_zona",
        PROFILE_PRODUCER,
        "results_mun_zone",
        1,
        ("candidatos",),
    ),
    # Wave 1 — independent (no título merge)
    TableFlow("partidos", PROFILE_PRODUCER, "parties", 1),
    TableFlow("vagas", PROFILE_PRODUCER, "vacancies", 1),
    TableFlow(
        "resultados_partido_secao", PROFILE_PRODUCER, "results_section", 1
    ),
    TableFlow(
        "resultados_partido_municipio_zona",
        PROFILE_PRODUCER,
        "results_mun_zone",
        1,
    ),
    TableFlow(
        "detalhes_votacao_secao", PROFILE_PRODUCER, "voting_details_section", 1
    ),
    TableFlow(
        "detalhes_votacao_municipio_zona",
        PROFILE_PRODUCER,
        "voting_details_mun_zone",
        1,
    ),
    TableFlow(
        "perfil_eleitorado_secao", PROFILE_PRODUCER, "voter_profile_section", 1
    ),
    TableFlow(
        "perfil_eleitorado_municipio_zona",
        PROFILE_PRODUCER,
        "voter_profile_mun_zone",
        1,
    ),
    TableFlow(
        "perfil_eleitorado_local_votacao",
        PROFILE_PRODUCER,
        "voter_profile_polling_place",
        1,
    ),
    # Wave 2 — Phase 3 rollups (pure dbt; ref() over the published zona table)
    TableFlow(
        "resultados_candidato_municipio",
        PROFILE_DBT_ROLLUP,
        None,
        2,
        ("resultados_candidato_municipio_zona",),
    ),
    TableFlow(
        "resultados_partido_municipio",
        PROFILE_DBT_ROLLUP,
        None,
        2,
        ("resultados_partido_municipio_zona",),
    ),
    TableFlow(
        "detalhes_votacao_municipio",
        PROFILE_DBT_ROLLUP,
        None,
        2,
        ("detalhes_votacao_municipio_zona",),
    ),
    # Wave 2 — Phase 3 python aggregations
    TableFlow(
        "resultados_candidato",
        PROFILE_PYTHON_AGG,
        "aggregate",
        2,
        ("resultados_candidato_municipio_zona", "candidatos"),
    ),
    TableFlow(
        "receitas_comite",
        PROFILE_PYTHON_AGG,
        "aggregate",
        2,
        ("receitas_candidato",),
    ),
    TableFlow(
        "receitas_orgao_partidario",
        PROFILE_PYTHON_AGG,
        "aggregate",
        2,
        ("receitas_candidato",),
    ),
]

TABLE_FLOW_BY_ID: dict[str, TableFlow] = {t.table_id: t for t in TABLE_FLOWS}
