"""
Flows for br_sp_saopaulo_geosampa_iptu.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_sp_saopaulo_geosampa_iptu.constants import (
    DATASET_ID,
    IPTU_TABLE_ID,
)
from pipelines.datasets.br_sp_saopaulo_geosampa_iptu.tasks import (
    iptu_extract_load_data,
    iptu_get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import (
    CheckThenExtractLoadPipeline,
    Etapa,
    deploy_tags,
)

_iptu_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=IPTU_TABLE_ID,
    get_latest_update=iptu_get_latest_update,
    extract_load_data=iptu_extract_load_data,
    date_format="%Y",
)


@flow(name=_iptu_pipeline.check_update_flow_name, log_prints=True)
def br_sp_saopaulo_geosampa_iptu_iptu_check_update() -> None:
    _iptu_pipeline.run_check_update()


br_sp_saopaulo_geosampa_iptu_iptu_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, IPTU_TABLE_ID
)
# toda segunda-feira
br_sp_saopaulo_geosampa_iptu_iptu_check_update.deploy_schedules = [
    Cron("47 9 * * 1", timezone="America/Sao_Paulo")
]


@flow(name=_iptu_pipeline.extract_and_load_flow_name, log_prints=True)
def br_sp_saopaulo_geosampa_iptu_iptu_download(download_params: dict) -> None:
    _iptu_pipeline.run_extract_and_load(download_params)


br_sp_saopaulo_geosampa_iptu_iptu_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, IPTU_TABLE_ID
)
_iptu_pipeline.extract_load_deployment = (
    br_sp_saopaulo_geosampa_iptu_iptu_download.fn.__name__
)
