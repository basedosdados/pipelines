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
from pipelines.datasets.br_ibge_ipca.tasks import (
    make_check_for_update,
    make_download_data,
)
from pipelines.utils.stage_dispatch import (
    CheckThenDownloadPipeline,
    Etapa,
    deploy_tags,
)


def _make_pipeline(table_id: str) -> CheckThenDownloadPipeline:
    return CheckThenDownloadPipeline(
        dataset_id=DATASET_ID,
        table_id=table_id,
        check_for_update=make_check_for_update(table_id),
        download_data=make_download_data(table_id),
        # Mesma granularidade do flow antigo (`_run_ibge_inflacao`, que já
        # compara coverage com date_format="%Y-%m" — o dado é mensal, sem dia).
        date_format="%Y-%m",
    )


_mes_brasil_pipeline = _make_pipeline(MES_BRASIL_TABLE_ID)
_mes_categoria_brasil_pipeline = _make_pipeline(MES_CATEGORIA_BRASIL_TABLE_ID)
_mes_categoria_rm_pipeline = _make_pipeline(MES_CATEGORIA_RM_TABLE_ID)
_mes_categoria_municipio_pipeline = _make_pipeline(
    MES_CATEGORIA_MUNICIPIO_TABLE_ID
)


# ──────────────────────────────────────────────────────────────────────────────
# mes_brasil
# ──────────────────────────────────────────────────────────────────────────────


@flow(name=_mes_brasil_pipeline.check_update_flow_name, log_prints=True)
def br_ibge_ipca_mes_brasil_check_update_flow() -> None:
    _mes_brasil_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ibge_ipca_mes_brasil_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_mes_brasil_pipeline.download_flow_name, log_prints=True)
def br_ibge_ipca_mes_brasil_download_flow(download_params: dict) -> None:
    _mes_brasil_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ibge_ipca_mes_brasil_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_mes_brasil_pipeline.download_deployment = (
    br_ibge_ipca_mes_brasil_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# mes_categoria_brasil
# ──────────────────────────────────────────────────────────────────────────────


@flow(
    name=_mes_categoria_brasil_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_ibge_ipca_mes_categoria_brasil_check_update_flow() -> None:
    _mes_categoria_brasil_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ibge_ipca_mes_categoria_brasil_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(
    name=_mes_categoria_brasil_pipeline.download_flow_name,
    log_prints=True,
)
def br_ibge_ipca_mes_categoria_brasil_download_flow(
    download_params: dict,
) -> None:
    _mes_categoria_brasil_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ibge_ipca_mes_categoria_brasil_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_mes_categoria_brasil_pipeline.download_deployment = (
    br_ibge_ipca_mes_categoria_brasil_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# mes_categoria_rm
# ──────────────────────────────────────────────────────────────────────────────


@flow(
    name=_mes_categoria_rm_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_ibge_ipca_mes_categoria_rm_check_update_flow() -> None:
    _mes_categoria_rm_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ibge_ipca_mes_categoria_rm_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(
    name=_mes_categoria_rm_pipeline.download_flow_name,
    log_prints=True,
)
def br_ibge_ipca_mes_categoria_rm_download_flow(
    download_params: dict,
) -> None:
    _mes_categoria_rm_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ibge_ipca_mes_categoria_rm_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_mes_categoria_rm_pipeline.download_deployment = (
    br_ibge_ipca_mes_categoria_rm_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# mes_categoria_municipio
# ──────────────────────────────────────────────────────────────────────────────


@flow(
    name=_mes_categoria_municipio_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_ibge_ipca_mes_categoria_municipio_check_update_flow() -> None:
    _mes_categoria_municipio_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ibge_ipca_mes_categoria_municipio_check_update_flow.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE)
)


@flow(
    name=_mes_categoria_municipio_pipeline.download_flow_name,
    log_prints=True,
)
def br_ibge_ipca_mes_categoria_municipio_download_flow(
    download_params: dict,
) -> None:
    _mes_categoria_municipio_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ibge_ipca_mes_categoria_municipio_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_mes_categoria_municipio_pipeline.download_deployment = (
    br_ibge_ipca_mes_categoria_municipio_download_flow.fn.__name__
)
