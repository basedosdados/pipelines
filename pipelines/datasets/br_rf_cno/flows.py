"""
Flows for br_rf_cno — Prefect 3.

Migrado pro pipeline orientado a eventos: check_update -> extract_and_load ->
build_and_promote, uma dupla de flows por tabela (microdados, vinculos, areas,
cnaes). Lógica específica do dataset mora em `tasks.py`, constantes em
`constants.py` — aqui só a fiação (`pipeline_factory` + `@flow`).

O antigo `_cno_flow`/`_run_rf` monolítico foi removido daqui; contexto da
fonte, do WAF e das particularidades por tabela: ver
`pipelines/datasets/br_rf_cno/README.md`.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_rf_cno.constants import (
    AREAS_TABLE_ID,
    CNAES_TABLE_ID,
    DATASET_ID,
    MICRODADOS_TABLE_ID,
    VINCULOS_TABLE_ID,
)
from pipelines.datasets.br_rf_cno.tasks import (
    make_extract_load_data,
    make_get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import (
    Etapa,
    deploy_tags,
    pipeline_factory,
)

make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
    # Mesma granularidade do flow antigo (`_run_rf`, date_format="%Y-%m-%d").
    date_format="%Y-%m-%d",
)

# ──────────────────────────────────────────────────────────────────────────────
# microdados
# ──────────────────────────────────────────────────────────────────────────────

_microdados_pipeline = make_pipeline(MICRODADOS_TABLE_ID)


@flow(name=_microdados_pipeline.check_update_flow_name, log_prints=True)
def br_rf_cno_microdados_check_update() -> None:
    _microdados_pipeline.run_check_update()


br_rf_cno_microdados_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, MICRODADOS_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_rf_cno_microdados_check_update.deploy_schedules = [
    Cron("5 4 * * 1-5", timezone="America/Sao_Paulo")
]


@flow(name=_microdados_pipeline.extract_and_load_flow_name, log_prints=True)
def br_rf_cno_microdados_download(download_params: dict) -> None:
    _microdados_pipeline.run_extract_and_load(download_params)


br_rf_cno_microdados_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, MICRODADOS_TABLE_ID
)
_microdados_pipeline.extract_load_deployment = (
    br_rf_cno_microdados_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# vinculos
# ──────────────────────────────────────────────────────────────────────────────

_vinculos_pipeline = make_pipeline(VINCULOS_TABLE_ID)


@flow(name=_vinculos_pipeline.check_update_flow_name, log_prints=True)
def br_rf_cno_vinculos_check_update() -> None:
    _vinculos_pipeline.run_check_update()


br_rf_cno_vinculos_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, VINCULOS_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_rf_cno_vinculos_check_update.deploy_schedules = [
    Cron("15 4 * * 1-5", timezone="America/Sao_Paulo")
]


@flow(name=_vinculos_pipeline.extract_and_load_flow_name, log_prints=True)
def br_rf_cno_vinculos_download(download_params: dict) -> None:
    _vinculos_pipeline.run_extract_and_load(download_params)


br_rf_cno_vinculos_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, VINCULOS_TABLE_ID
)
_vinculos_pipeline.extract_load_deployment = (
    br_rf_cno_vinculos_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# areas
# ──────────────────────────────────────────────────────────────────────────────

_areas_pipeline = make_pipeline(AREAS_TABLE_ID)


@flow(name=_areas_pipeline.check_update_flow_name, log_prints=True)
def br_rf_cno_areas_check_update() -> None:
    _areas_pipeline.run_check_update()


br_rf_cno_areas_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, AREAS_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_rf_cno_areas_check_update.deploy_schedules = [
    Cron("25 4 * * 1-5", timezone="America/Sao_Paulo")
]


@flow(name=_areas_pipeline.extract_and_load_flow_name, log_prints=True)
def br_rf_cno_areas_download(download_params: dict) -> None:
    _areas_pipeline.run_extract_and_load(download_params)


br_rf_cno_areas_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, AREAS_TABLE_ID
)
_areas_pipeline.extract_load_deployment = br_rf_cno_areas_download.fn.__name__


# ──────────────────────────────────────────────────────────────────────────────
# cnaes
# ──────────────────────────────────────────────────────────────────────────────

_cnaes_pipeline = make_pipeline(CNAES_TABLE_ID)


@flow(name=_cnaes_pipeline.check_update_flow_name, log_prints=True)
def br_rf_cno_cnaes_check_update() -> None:
    _cnaes_pipeline.run_check_update()


br_rf_cno_cnaes_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, CNAES_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_rf_cno_cnaes_check_update.deploy_schedules = [
    Cron("35 4 * * 1-5", timezone="America/Sao_Paulo")
]


@flow(name=_cnaes_pipeline.extract_and_load_flow_name, log_prints=True)
def br_rf_cno_cnaes_download(download_params: dict) -> None:
    _cnaes_pipeline.run_extract_and_load(download_params)


br_rf_cno_cnaes_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, CNAES_TABLE_ID
)
_cnaes_pipeline.extract_load_deployment = br_rf_cno_cnaes_download.fn.__name__
