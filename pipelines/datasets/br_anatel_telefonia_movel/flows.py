"""Flows de br_anatel_telefonia_movel — Prefect 3, modelo em estágios."""

from prefect.schedules import Cron

from pipelines.datasets.br_anatel_telefonia_movel.constants import (
    DATASET_ID,
    DENSIDADE_BRASIL_TABLE_ID,
    DENSIDADE_MUNICIPIO_TABLE_ID,
    DENSIDADE_UF_TABLE_ID,
    MICRODADOS_TABLE_ID,
)
from pipelines.datasets.br_anatel_telefonia_movel.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

_microdados_pipeline = make_pipeline(MICRODADOS_TABLE_ID)


@flow(name=_microdados_pipeline.check_update_flow_name, log_prints=True)
def br_anatel_telefonia_movel_microdados_check_update() -> None:
    _microdados_pipeline.run_check_update()


br_anatel_telefonia_movel_microdados_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, MICRODADOS_TABLE_ID
)
br_anatel_telefonia_movel_microdados_check_update.deploy_schedules = [
    Cron("32 16 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_microdados_pipeline.extract_and_load_flow_name, log_prints=True)
def br_anatel_telefonia_movel_microdados_download(
    download_params: dict,
) -> None:
    _microdados_pipeline.run_extract_and_load(download_params)


br_anatel_telefonia_movel_microdados_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, MICRODADOS_TABLE_ID
)
_microdados_pipeline.extract_load_deployment = (
    br_anatel_telefonia_movel_microdados_download.fn.__name__
)


_densidade_municipio_pipeline = make_pipeline(DENSIDADE_MUNICIPIO_TABLE_ID)


@flow(
    name=_densidade_municipio_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_anatel_telefonia_movel_densidade_municipio_check_update() -> None:
    _densidade_municipio_pipeline.run_check_update()


br_anatel_telefonia_movel_densidade_municipio_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, DENSIDADE_MUNICIPIO_TABLE_ID)
)
br_anatel_telefonia_movel_densidade_municipio_check_update.deploy_schedules = [
    Cron("36 16 * * *", timezone="America/Sao_Paulo")
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
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, DENSIDADE_MUNICIPIO_TABLE_ID
    )
)
_densidade_municipio_pipeline.extract_load_deployment = (
    br_anatel_telefonia_movel_densidade_municipio_download.fn.__name__
)


_densidade_uf_pipeline = make_pipeline(DENSIDADE_UF_TABLE_ID)


@flow(name=_densidade_uf_pipeline.check_update_flow_name, log_prints=True)
def br_anatel_telefonia_movel_densidade_uf_check_update() -> None:
    _densidade_uf_pipeline.run_check_update()


br_anatel_telefonia_movel_densidade_uf_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, DENSIDADE_UF_TABLE_ID
)
br_anatel_telefonia_movel_densidade_uf_check_update.deploy_schedules = [
    Cron("40 16 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_densidade_uf_pipeline.extract_and_load_flow_name, log_prints=True)
def br_anatel_telefonia_movel_densidade_uf_download(
    download_params: dict,
) -> None:
    _densidade_uf_pipeline.run_extract_and_load(download_params)


br_anatel_telefonia_movel_densidade_uf_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, DENSIDADE_UF_TABLE_ID
)
_densidade_uf_pipeline.extract_load_deployment = (
    br_anatel_telefonia_movel_densidade_uf_download.fn.__name__
)


_densidade_brasil_pipeline = make_pipeline(DENSIDADE_BRASIL_TABLE_ID)


@flow(name=_densidade_brasil_pipeline.check_update_flow_name, log_prints=True)
def br_anatel_telefonia_movel_densidade_brasil_check_update() -> None:
    _densidade_brasil_pipeline.run_check_update()


br_anatel_telefonia_movel_densidade_brasil_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, DENSIDADE_BRASIL_TABLE_ID)
)
br_anatel_telefonia_movel_densidade_brasil_check_update.deploy_schedules = [
    Cron("44 16 * * *", timezone="America/Sao_Paulo")
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
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, DENSIDADE_BRASIL_TABLE_ID
)
_densidade_brasil_pipeline.extract_load_deployment = (
    br_anatel_telefonia_movel_densidade_brasil_download.fn.__name__
)
