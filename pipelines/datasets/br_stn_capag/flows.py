"""
Flows for br_stn_capag — Prefect 3.
"""

from pipelines.datasets.br_stn_capag.constants import (
    BASE_CALCULO_TABLE_ID,
    DATASET_ID,
    ESTADOS_TABLE_ID,
    MUNICIPIOS_TABLE_ID,
)
from pipelines.datasets.br_stn_capag.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

_CHECK_UPDATE_SCHEDULE = []

# municipios
_municipios_pipeline = make_pipeline(MUNICIPIOS_TABLE_ID)


@flow(name=_municipios_pipeline.check_update_flow_name, log_prints=True)
def br_stn_capag_municipios_check_update() -> None:
    _municipios_pipeline.run_check_update()


br_stn_capag_municipios_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, MUNICIPIOS_TABLE_ID
)
# br_stn_capag_municipios_check_update.deploy_schedules = _CHECK_UPDATE_SCHEDULE


@flow(name=_municipios_pipeline.extract_and_load_flow_name, log_prints=True)
def br_stn_capag_municipios_download(download_params: dict) -> None:
    _municipios_pipeline.run_extract_and_load(download_params)


br_stn_capag_municipios_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, MUNICIPIOS_TABLE_ID
)
_municipios_pipeline.extract_load_deployment = (
    br_stn_capag_municipios_download.fn.__name__
)


# estados
_estados_pipeline = make_pipeline(ESTADOS_TABLE_ID)


@flow(name=_estados_pipeline.check_update_flow_name, log_prints=True)
def br_stn_capag_estados_check_update() -> None:
    _estados_pipeline.run_check_update()


br_stn_capag_estados_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, ESTADOS_TABLE_ID
)
# br_stn_capag_estados_check_update.deploy_schedules = _CHECK_UPDATE_SCHEDULE


@flow(name=_estados_pipeline.extract_and_load_flow_name, log_prints=True)
def br_stn_capag_estados_download(download_params: dict) -> None:
    _estados_pipeline.run_extract_and_load(download_params)


br_stn_capag_estados_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, ESTADOS_TABLE_ID
)
_estados_pipeline.extract_load_deployment = (
    br_stn_capag_estados_download.fn.__name__
)


# municipios_base_calculo
_base_calculo_pipeline = make_pipeline(BASE_CALCULO_TABLE_ID)


@flow(name=_base_calculo_pipeline.check_update_flow_name, log_prints=True)
def br_stn_capag_municipios_base_calculo_check_update() -> None:
    _base_calculo_pipeline.run_check_update()


br_stn_capag_municipios_base_calculo_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, BASE_CALCULO_TABLE_ID
)
# br_stn_capag_municipios_base_calculo_check_update.deploy_schedules = _CHECK_UPDATE_SCHEDULE


@flow(name=_base_calculo_pipeline.extract_and_load_flow_name, log_prints=True)
def br_stn_capag_municipios_base_calculo_download(
    download_params: dict,
) -> None:
    _base_calculo_pipeline.run_extract_and_load(download_params)


br_stn_capag_municipios_base_calculo_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, BASE_CALCULO_TABLE_ID
)
_base_calculo_pipeline.extract_load_deployment = (
    br_stn_capag_municipios_base_calculo_download.fn.__name__
)
