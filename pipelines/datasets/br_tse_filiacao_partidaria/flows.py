"""Flows de br_tse_filiacao_partidaria — Prefect 3, modelo em estágios."""

from prefect.schedules import Cron

from pipelines.datasets.br_tse_filiacao_partidaria.constants import (
    DATASET_ID,
    MICRODADOS_TABLE_ID,
)
from pipelines.datasets.br_tse_filiacao_partidaria.tasks import (
    extract_load_data,
    get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import (
    CheckThenExtractLoadPipeline,
    Etapa,
    deploy_tags,
)

_microdados_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=MICRODADOS_TABLE_ID,
    get_latest_update=get_latest_update,
    extract_load_data=extract_load_data,
)


@flow(name=_microdados_pipeline.check_update_flow_name, log_prints=True)
def br_tse_filiacao_partidaria_microdados_check_update() -> None:
    _microdados_pipeline.run_check_update()


br_tse_filiacao_partidaria_microdados_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, MICRODADOS_TABLE_ID
)
br_tse_filiacao_partidaria_microdados_check_update.deploy_schedules = [
    Cron("23 1 1 * *", timezone="America/Sao_Paulo")
]


@flow(name=_microdados_pipeline.extract_and_load_flow_name, log_prints=True)
def br_tse_filiacao_partidaria_microdados_download(
    download_params: dict,
) -> None:
    _microdados_pipeline.run_extract_and_load(download_params)


br_tse_filiacao_partidaria_microdados_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, MICRODADOS_TABLE_ID
)
_microdados_pipeline.extract_load_deployment = (
    br_tse_filiacao_partidaria_microdados_download.fn.__name__
)
