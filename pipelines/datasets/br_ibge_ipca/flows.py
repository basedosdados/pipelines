"""
Flows para br_ibge_ipca — Prefect 3.

Migrado por completo pro pipeline orientado a eventos (issue #1867):
check_update -> download -> mat_test, uma dupla de flows por tabela.
Lógica específica do dataset mora em `tasks.py`, constantes em
`constants.py` — aqui só a fiação (`CheckThenDownloadPipeline` + `@flow`).

O antigo `_ipca_flow`/`_run_ibge_inflacao` monolítico segue existindo em
`pipelines/crawler/ibge_inflacao/flows.py`, ainda usado por
`br_ibge_ipca15`/`br_ibge_inpc` (não migrados ainda) — não removido daqui.
"""

from prefect import flow

from pipelines.datasets.br_ibge_ipca.constants import (
    DATASET_ID,
    MES_BRASIL_TABLE_ID,
    MES_CATEGORIA_BRASIL_TABLE_ID,
    MES_CATEGORIA_MUNICIPIO_TABLE_ID,
    MES_CATEGORIA_RM_TABLE_ID,
)
from pipelines.datasets.br_ibge_ipca.tasks import make_pipeline
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# ──────────────────────────────────────────────────────────────────────────────
# mes_brasil
# check_update: br_ibge_ipca__mes_brasil
# download: br_ibge_ipca__mes_brasil
# ──────────────────────────────────────────────────────────────────────────────

_mes_brasil_pipeline = make_pipeline(MES_BRASIL_TABLE_ID)


@flow(name=_mes_brasil_pipeline.check_update_flow_name, log_prints=True)
def br_ibge_ipca_mes_brasil_check_update() -> None:
    _mes_brasil_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ibge_ipca_mes_brasil_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_mes_brasil_pipeline.download_flow_name, log_prints=True)
def br_ibge_ipca_mes_brasil_download(download_params: dict) -> None:
    _mes_brasil_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ibge_ipca_mes_brasil_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_mes_brasil_pipeline.download_deployment = (
    br_ibge_ipca_mes_brasil_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# mes_categoria_brasil
# check_update: br_ibge_ipca__mes_categoria_brasil
# download: br_ibge_ipca__mes_categoria_brasil
# ──────────────────────────────────────────────────────────────────────────────

_mes_categoria_brasil_pipeline = make_pipeline(MES_CATEGORIA_BRASIL_TABLE_ID)


@flow(
    name=_mes_categoria_brasil_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_ibge_ipca_mes_categoria_brasil_check_update() -> None:
    _mes_categoria_brasil_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ibge_ipca_mes_categoria_brasil_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(
    name=_mes_categoria_brasil_pipeline.download_flow_name,
    log_prints=True,
)
def br_ibge_ipca_mes_categoria_brasil_download(
    download_params: dict,
) -> None:
    _mes_categoria_brasil_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ibge_ipca_mes_categoria_brasil_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_mes_categoria_brasil_pipeline.download_deployment = (
    br_ibge_ipca_mes_categoria_brasil_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# mes_categoria_rm
# check_update: br_ibge_ipca__mes_categoria_rm
# download: br_ibge_ipca__mes_categoria_rm
# ──────────────────────────────────────────────────────────────────────────────

_mes_categoria_rm_pipeline = make_pipeline(MES_CATEGORIA_RM_TABLE_ID)


@flow(
    name=_mes_categoria_rm_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_ibge_ipca_mes_categoria_rm_check_update() -> None:
    _mes_categoria_rm_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ibge_ipca_mes_categoria_rm_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(
    name=_mes_categoria_rm_pipeline.download_flow_name,
    log_prints=True,
)
def br_ibge_ipca_mes_categoria_rm_download(
    download_params: dict,
) -> None:
    _mes_categoria_rm_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ibge_ipca_mes_categoria_rm_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_mes_categoria_rm_pipeline.download_deployment = (
    br_ibge_ipca_mes_categoria_rm_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# mes_categoria_municipio
# check_update: br_ibge_ipca__mes_categoria_municipio
# download: br_ibge_ipca__mes_categoria_municipio
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


# pyrefly: ignore [missing-attribute]
br_ibge_ipca_mes_categoria_municipio_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(
    name=_mes_categoria_municipio_pipeline.download_flow_name,
    log_prints=True,
)
def br_ibge_ipca_mes_categoria_municipio_download(
    download_params: dict,
) -> None:
    _mes_categoria_municipio_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ibge_ipca_mes_categoria_municipio_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_mes_categoria_municipio_pipeline.download_deployment = (
    br_ibge_ipca_mes_categoria_municipio_download.fn.__name__
)
