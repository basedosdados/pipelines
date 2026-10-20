"""Flows de br_anatel_telefonia_movel — Prefect 3, modelo em estágios."""

from prefect.schedules import Cron

from pipelines.datasets.br_anatel_telefonia_movel.constants import (
    constants as anatel_constants,
)
from pipelines.datasets.br_anatel_telefonia_movel.tasks import (
    make_extract_load_data,
    make_get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import (
    CheckThenExtractLoadPipeline,
    Etapa,
    deploy_tags,
)

_microdados_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=anatel_constants.DATASET_ID.value,
    table_id="microdados",
    get_latest_update=make_get_latest_update("microdados"),
    extract_load_data=make_extract_load_data("microdados"),
    date_format="%Y-%m",
)


@flow(name=_microdados_pipeline.check_update_flow_name, log_prints=True)
def br_anatel_telefonia_movel_microdados_check_update() -> None:
    _microdados_pipeline.run_check_update()


br_anatel_telefonia_movel_microdados_check_update.deploy_tags = deploy_tags(
    anatel_constants.DATASET_ID.value, Etapa.CHECK_UPDATE, "microdados"
)
br_anatel_telefonia_movel_microdados_check_update.deploy_schedules = [
    Cron("30 16 * * 1", timezone="America/Sao_Paulo")
]


@flow(name=_microdados_pipeline.extract_and_load_flow_name, log_prints=True)
def br_anatel_telefonia_movel_microdados_download(
    download_params: dict,
) -> None:
    _microdados_pipeline.run_extract_and_load(download_params)


br_anatel_telefonia_movel_microdados_download.deploy_tags = deploy_tags(
    anatel_constants.DATASET_ID.value, Etapa.EXTRACT_AND_LOAD, "microdados"
)
_microdados_pipeline.extract_load_deployment = (
    br_anatel_telefonia_movel_microdados_download.fn.__name__
)


_densidade_municipio_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=anatel_constants.DATASET_ID.value,
    table_id="densidade_municipio",
    get_latest_update=make_get_latest_update("densidade_municipio"),
    extract_load_data=make_extract_load_data("densidade_municipio"),
    date_format="%Y-%m",
)


@flow(
    name=_densidade_municipio_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_anatel_telefonia_movel_densidade_municipio_check_update() -> None:
    _densidade_municipio_pipeline.run_check_update()


br_anatel_telefonia_movel_densidade_municipio_check_update.deploy_tags = (
    deploy_tags(
        anatel_constants.DATASET_ID.value,
        Etapa.CHECK_UPDATE,
        "densidade_municipio",
    )
)
br_anatel_telefonia_movel_densidade_municipio_check_update.deploy_schedules = [
    Cron("30 16 * * 2", timezone="America/Sao_Paulo")
]


@flow(
    name=_densidade_municipio_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_anatel_telefonia_movel_densidade_municipio_download(
    download_params: dict,
) -> None:
    _densidade_municipio_pipeline.run_extract_and_load(download_params)


br_anatel_telefonia_movel_densidade_municipio_download.deploy_tags = (
    deploy_tags(
        anatel_constants.DATASET_ID.value,
        Etapa.EXTRACT_AND_LOAD,
        "densidade_municipio",
    )
)
_densidade_municipio_pipeline.extract_load_deployment = (
    br_anatel_telefonia_movel_densidade_municipio_download.fn.__name__
)


_densidade_uf_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=anatel_constants.DATASET_ID.value,
    table_id="densidade_uf",
    get_latest_update=make_get_latest_update("densidade_uf"),
    extract_load_data=make_extract_load_data("densidade_uf"),
    date_format="%Y-%m",
)


@flow(name=_densidade_uf_pipeline.check_update_flow_name, log_prints=True)
def br_anatel_telefonia_movel_densidade_uf_check_update() -> None:
    _densidade_uf_pipeline.run_check_update()


br_anatel_telefonia_movel_densidade_uf_check_update.deploy_tags = deploy_tags(
    anatel_constants.DATASET_ID.value, Etapa.CHECK_UPDATE, "densidade_uf"
)
br_anatel_telefonia_movel_densidade_uf_check_update.deploy_schedules = [
    Cron("30 16 * * 3", timezone="America/Sao_Paulo")
]


@flow(name=_densidade_uf_pipeline.extract_and_load_flow_name, log_prints=True)
def br_anatel_telefonia_movel_densidade_uf_download(
    download_params: dict,
) -> None:
    _densidade_uf_pipeline.run_extract_and_load(download_params)


br_anatel_telefonia_movel_densidade_uf_download.deploy_tags = deploy_tags(
    anatel_constants.DATASET_ID.value, Etapa.EXTRACT_AND_LOAD, "densidade_uf"
)
_densidade_uf_pipeline.extract_load_deployment = (
    br_anatel_telefonia_movel_densidade_uf_download.fn.__name__
)


_densidade_brasil_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=anatel_constants.DATASET_ID.value,
    table_id="densidade_brasil",
    get_latest_update=make_get_latest_update("densidade_brasil"),
    extract_load_data=make_extract_load_data("densidade_brasil"),
    date_format="%Y-%m",
)


@flow(name=_densidade_brasil_pipeline.check_update_flow_name, log_prints=True)
def br_anatel_telefonia_movel_densidade_brasil_check_update() -> None:
    _densidade_brasil_pipeline.run_check_update()


br_anatel_telefonia_movel_densidade_brasil_check_update.deploy_tags = (
    deploy_tags(
        anatel_constants.DATASET_ID.value,
        Etapa.CHECK_UPDATE,
        "densidade_brasil",
    )
)
br_anatel_telefonia_movel_densidade_brasil_check_update.deploy_schedules = [
    Cron("30 16 * * 4", timezone="America/Sao_Paulo")
]


@flow(
    name=_densidade_brasil_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_anatel_telefonia_movel_densidade_brasil_download(
    download_params: dict,
) -> None:
    _densidade_brasil_pipeline.run_extract_and_load(download_params)


br_anatel_telefonia_movel_densidade_brasil_download.deploy_tags = deploy_tags(
    anatel_constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    "densidade_brasil",
)
_densidade_brasil_pipeline.extract_load_deployment = (
    br_anatel_telefonia_movel_densidade_brasil_download.fn.__name__
)
