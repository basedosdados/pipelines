"""
Flows para br_ms_sia — Prefect 3.

Migrado por completo pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote, uma dupla de flows por
tabela. Lógica específica do dataset mora em `tasks.py`, constantes em
`constants.py` — aqui só a fiação (`CheckThenExtractLoadPipeline` + `@flow`).

O antigo `_sia_flow`/`_run_siasus` monolítico segue existindo como
`_run_dbf_to_parquet` em `pipelines/crawler/datasus/flows.py`, ainda usado por
br_ms_sih/br_ms_sinan (não migrados ainda) — não removido daqui.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_ms_sia.constants import (
    DATASET_ID,
    PRODUCAO_AMBULATORIAL_TABLE_ID,
    PSICOSSOCIAL_TABLE_ID,
)
from pipelines.datasets.br_ms_sia.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# ──────────────────────────────────────────────────────────────────────────────
# producao_ambulatorial
# check_update: br_ms_sia__producao_ambulatorial
# download: br_ms_sia__producao_ambulatorial
# ──────────────────────────────────────────────────────────────────────────────

_producao_ambulatorial_pipeline = make_pipeline(PRODUCAO_AMBULATORIAL_TABLE_ID)


@flow(
    name=_producao_ambulatorial_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_ms_sia_producao_ambulatorial_check_update() -> None:
    _producao_ambulatorial_pipeline.run_check_update()


br_ms_sia_producao_ambulatorial_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, PRODUCAO_AMBULATORIAL_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_ms_sia_producao_ambulatorial_check_update.deploy_schedules = [
    Cron("0 21 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_producao_ambulatorial_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_ms_sia_producao_ambulatorial_download(download_params: dict) -> None:
    _producao_ambulatorial_pipeline.run_extract_and_load(download_params)


br_ms_sia_producao_ambulatorial_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, PRODUCAO_AMBULATORIAL_TABLE_ID
)
_producao_ambulatorial_pipeline.extract_load_deployment = (
    br_ms_sia_producao_ambulatorial_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# psicossocial
# check_update: br_ms_sia__psicossocial
# download: br_ms_sia__psicossocial
# ──────────────────────────────────────────────────────────────────────────────

_psicossocial_pipeline = make_pipeline(PSICOSSOCIAL_TABLE_ID)


@flow(name=_psicossocial_pipeline.check_update_flow_name, log_prints=True)
def br_ms_sia_psicossocial_check_update() -> None:
    _psicossocial_pipeline.run_check_update()


br_ms_sia_psicossocial_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, PSICOSSOCIAL_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_ms_sia_psicossocial_check_update.deploy_schedules = [
    Cron("0 7 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_psicossocial_pipeline.extract_and_load_flow_name, log_prints=True)
def br_ms_sia_psicossocial_download(download_params: dict) -> None:
    _psicossocial_pipeline.run_extract_and_load(download_params)


br_ms_sia_psicossocial_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, PSICOSSOCIAL_TABLE_ID
)
_psicossocial_pipeline.extract_load_deployment = (
    br_ms_sia_psicossocial_download.fn.__name__
)
