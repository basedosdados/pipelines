"""
Flows para br_ms_sih — Prefect 3.

Migrado por completo pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote, uma dupla de flows por
tabela. Lógica específica do dataset mora em `tasks.py`, constantes em
`constants.py` — aqui só a fiação (`CheckThenExtractLoadPipeline` + `@flow`).

O antigo `_run_sihsus`/`_run_dbf_to_parquet` monolítico segue existindo em
`pipelines/crawler/datasus/flows.py`, ainda usado por `br_ms_sia` (não
migrado ainda) — não removido daqui.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_ms_sih.constants import (
    AIHS_REDUZIDAS_TABLE_ID,
    DATASET_ID,
    SERVICOS_PROFISSIONAIS_TABLE_ID,
)
from pipelines.datasets.br_ms_sih.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# ──────────────────────────────────────────────────────────────────────────────
# servicos_profissionais
# check_update: br_ms_sih__servicos_profissionais
# download: br_ms_sih__servicos_profissionais
# ──────────────────────────────────────────────────────────────────────────────

_servicos_profissionais_pipeline = make_pipeline(
    SERVICOS_PROFISSIONAIS_TABLE_ID
)


@flow(
    name=_servicos_profissionais_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_ms_sih_servicos_profissionais_check_update() -> None:
    _servicos_profissionais_pipeline.run_check_update()


br_ms_sih_servicos_profissionais_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, SERVICOS_PROFISSIONAIS_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_ms_sih_servicos_profissionais_check_update.deploy_schedules = [
    Cron("30 3 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_servicos_profissionais_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_ms_sih_servicos_profissionais_download(download_params: dict) -> None:
    _servicos_profissionais_pipeline.run_extract_and_load(download_params)


br_ms_sih_servicos_profissionais_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, SERVICOS_PROFISSIONAIS_TABLE_ID
)
_servicos_profissionais_pipeline.extract_load_deployment = (
    br_ms_sih_servicos_profissionais_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# aihs_reduzidas
# check_update: br_ms_sih__aihs_reduzidas
# download: br_ms_sih__aihs_reduzidas
# ──────────────────────────────────────────────────────────────────────────────

_aihs_reduzidas_pipeline = make_pipeline(AIHS_REDUZIDAS_TABLE_ID)


@flow(name=_aihs_reduzidas_pipeline.check_update_flow_name, log_prints=True)
def br_ms_sih_aihs_reduzidas_check_update() -> None:
    _aihs_reduzidas_pipeline.run_check_update()


br_ms_sih_aihs_reduzidas_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, AIHS_REDUZIDAS_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_ms_sih_aihs_reduzidas_check_update.deploy_schedules = [
    Cron("30 6 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_aihs_reduzidas_pipeline.extract_and_load_flow_name, log_prints=True
)
def br_ms_sih_aihs_reduzidas_download(download_params: dict) -> None:
    _aihs_reduzidas_pipeline.run_extract_and_load(download_params)


br_ms_sih_aihs_reduzidas_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, AIHS_REDUZIDAS_TABLE_ID
)
_aihs_reduzidas_pipeline.extract_load_deployment = (
    br_ms_sih_aihs_reduzidas_download.fn.__name__
)
