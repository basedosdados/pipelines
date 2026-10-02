"""
Flows para br_ibge_ipca — Prefect 3.

Migrado por completo pro pipeline orientado a eventos:
check_update -> extract_and_load -> build_and_promote, uma dupla de flows por tabela.
Lógica específica do dataset mora em `tasks.py`, constantes em
`constants.py` — aqui só a fiação (`CheckThenExtractLoadPipeline` + `@flow`).

O antigo `_ipca_flow`/`_run_ibge_inflacao` monolítico segue existindo em
`pipelines/crawler/ibge_inflacao/flows.py`, ainda usado por
`br_ibge_ipca15`/`br_ibge_inpc` (não migrados ainda) — não removido daqui.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_ibge_ipca.constants import (
    DATASET_ID,
    MES_BRASIL_TABLE_ID,
    MES_CATEGORIA_BRASIL_TABLE_ID,
    MES_CATEGORIA_MUNICIPIO_TABLE_ID,
    MES_CATEGORIA_RM_TABLE_ID,
)
from pipelines.datasets.br_ibge_ipca.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# ──────────────────────────────────────────────────────────────────────────────
# mes_brasil
# check_update: br_ibge_ipca__mes_brasil
# extract_and_load: br_ibge_ipca__mes_brasil
# ──────────────────────────────────────────────────────────────────────────────

_mes_brasil_pipeline = make_pipeline(MES_BRASIL_TABLE_ID)


@flow(name=_mes_brasil_pipeline.check_update_flow_name, log_prints=True)
def br_ibge_ipca_mes_brasil_check_update() -> None:
    _mes_brasil_pipeline.run_check_update()


br_ibge_ipca_mes_brasil_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, MES_BRASIL_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_ibge_ipca_mes_brasil_check_update.deploy_schedules = [
    Cron("40 14 8,9,10,11,12,13 * *", timezone="America/Sao_Paulo")
]


@flow(name=_mes_brasil_pipeline.extract_and_load_flow_name, log_prints=True)
def br_ibge_ipca_mes_brasil_download(download_params: dict) -> None:
    _mes_brasil_pipeline.run_extract_and_load(download_params)


br_ibge_ipca_mes_brasil_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, MES_BRASIL_TABLE_ID
)
_mes_brasil_pipeline.extract_load_deployment = (
    br_ibge_ipca_mes_brasil_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# mes_categoria_brasil
# check_update: br_ibge_ipca__mes_categoria_brasil
# extract_and_load: br_ibge_ipca__mes_categoria_brasil
# ──────────────────────────────────────────────────────────────────────────────

_mes_categoria_brasil_pipeline = make_pipeline(MES_CATEGORIA_BRASIL_TABLE_ID)


@flow(
    name=_mes_categoria_brasil_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_ibge_ipca_mes_categoria_brasil_check_update() -> None:
    _mes_categoria_brasil_pipeline.run_check_update()


br_ibge_ipca_mes_categoria_brasil_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, MES_CATEGORIA_BRASIL_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_ibge_ipca_mes_categoria_brasil_check_update.deploy_schedules = [
    Cron("30 14 8,9,10,11,12,13 * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_mes_categoria_brasil_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_ibge_ipca_mes_categoria_brasil_download(
    download_params: dict,
) -> None:
    _mes_categoria_brasil_pipeline.run_extract_and_load(download_params)


br_ibge_ipca_mes_categoria_brasil_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, MES_CATEGORIA_BRASIL_TABLE_ID
)
_mes_categoria_brasil_pipeline.extract_load_deployment = (
    br_ibge_ipca_mes_categoria_brasil_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# mes_categoria_rm
# check_update: br_ibge_ipca__mes_categoria_rm
# extract_and_load: br_ibge_ipca__mes_categoria_rm
# ──────────────────────────────────────────────────────────────────────────────

_mes_categoria_rm_pipeline = make_pipeline(MES_CATEGORIA_RM_TABLE_ID)


@flow(
    name=_mes_categoria_rm_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_ibge_ipca_mes_categoria_rm_check_update() -> None:
    _mes_categoria_rm_pipeline.run_check_update()


br_ibge_ipca_mes_categoria_rm_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, MES_CATEGORIA_RM_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_ibge_ipca_mes_categoria_rm_check_update.deploy_schedules = [
    Cron("20 14 8,9,10,11,12,13 * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_mes_categoria_rm_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_ibge_ipca_mes_categoria_rm_download(
    download_params: dict,
) -> None:
    _mes_categoria_rm_pipeline.run_extract_and_load(download_params)


br_ibge_ipca_mes_categoria_rm_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, MES_CATEGORIA_RM_TABLE_ID
)
_mes_categoria_rm_pipeline.extract_load_deployment = (
    br_ibge_ipca_mes_categoria_rm_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# mes_categoria_municipio
# check_update: br_ibge_ipca__mes_categoria_municipio
# extract_and_load: br_ibge_ipca__mes_categoria_municipio
# ──────────────────────────────────────────────────────────────────────────────

_mes_categoria_municipio_pipeline = make_pipeline(
    MES_CATEGORIA_MUNICIPIO_TABLE_ID
)


@flow(
    name=_mes_categoria_municipio_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_ibge_ipca_mes_categoria_municipio_check_update() -> None:
    _mes_categoria_municipio_pipeline.run_check_update()


br_ibge_ipca_mes_categoria_municipio_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, MES_CATEGORIA_MUNICIPIO_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_ibge_ipca_mes_categoria_municipio_check_update.deploy_schedules = [
    Cron("50 14 8,9,10,11,12,13 * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_mes_categoria_municipio_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_ibge_ipca_mes_categoria_municipio_download(
    download_params: dict,
) -> None:
    _mes_categoria_municipio_pipeline.run_extract_and_load(download_params)


br_ibge_ipca_mes_categoria_municipio_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, MES_CATEGORIA_MUNICIPIO_TABLE_ID
)
_mes_categoria_municipio_pipeline.extract_load_deployment = (
    br_ibge_ipca_mes_categoria_municipio_download.fn.__name__
)
