"""
Flows de br_inmet_bdmep: check_update e extract_and_load de cada tabela.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_inmet_bdmep.constants import (
    DATASET_ID,
    ESTACAO_TABLE_ID,
    MICRODADOS_TABLE_ID,
)
from pipelines.datasets.br_inmet_bdmep.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

_microdados_pipeline = make_pipeline(MICRODADOS_TABLE_ID)


@flow(name=_microdados_pipeline.check_update_flow_name, log_prints=True)
def br_inmet_bdmep_microdados_check_update() -> None:
    _microdados_pipeline.run_check_update()


br_inmet_bdmep_microdados_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, MICRODADOS_TABLE_ID
)
br_inmet_bdmep_microdados_check_update.deploy_schedules = [
    Cron("0 22 * * 1-5", timezone="America/Sao_Paulo")
]


@flow(name=_microdados_pipeline.extract_and_load_flow_name, log_prints=True)
def br_inmet_bdmep_microdados_download(download_params: dict) -> None:
    _microdados_pipeline.run_extract_and_load(download_params)


br_inmet_bdmep_microdados_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, MICRODADOS_TABLE_ID
)
_microdados_pipeline.extract_load_deployment = (
    br_inmet_bdmep_microdados_download.fn.__name__
)


_estacao_pipeline = make_pipeline(ESTACAO_TABLE_ID)


@flow(name=_estacao_pipeline.check_update_flow_name, log_prints=True)
def br_inmet_bdmep_estacao_check_update() -> None:
    _estacao_pipeline.run_check_update()


br_inmet_bdmep_estacao_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, ESTACAO_TABLE_ID
)
br_inmet_bdmep_estacao_check_update.deploy_schedules = [
    Cron("5 18 * * 1-5", timezone="America/Sao_Paulo")
]


@flow(name=_estacao_pipeline.extract_and_load_flow_name, log_prints=True)
def br_inmet_bdmep_estacao_download(download_params: dict) -> None:
    _estacao_pipeline.run_extract_and_load(download_params)


br_inmet_bdmep_estacao_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, ESTACAO_TABLE_ID
)
_estacao_pipeline.extract_load_deployment = (
    br_inmet_bdmep_estacao_download.fn.__name__
)
