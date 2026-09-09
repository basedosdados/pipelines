"""
Flows para br_me_comex_stat — Prefect 3.

Migrado por completo pro pipeline orientado a eventos (issue #1867):
check_update -> download -> mat_test, uma dupla de flows por tabela.
Lógica específica do dataset mora em `tasks.py`, constantes em
`constants.py` — aqui só a fiação (`CheckThenDownloadPipeline` + `@flow`).
"""

from prefect import flow

from pipelines.datasets.br_me_comex_stat.constants import (
    DATASET_ID,
    MUNICIPIO_EXPORTACAO_TABLE_ID,
    MUNICIPIO_IMPORTACAO_TABLE_ID,
    NCM_EXPORTACAO_TABLE_ID,
    NCM_IMPORTACAO_TABLE_ID,
)
from pipelines.datasets.br_me_comex_stat.tasks import (
    br_me_comex_stat_check_for_update,
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
        # Mesma função pras 4 tabelas -- a fonte de check é única,
        # compartilhada (ver banner em tasks.py).
        check_for_update=br_me_comex_stat_check_for_update,
        download_data=make_download_data(table_id),
        date_format="%Y-%m",
    )


_municipio_exportacao_pipeline = _make_pipeline(MUNICIPIO_EXPORTACAO_TABLE_ID)
_municipio_importacao_pipeline = _make_pipeline(MUNICIPIO_IMPORTACAO_TABLE_ID)
_ncm_exportacao_pipeline = _make_pipeline(NCM_EXPORTACAO_TABLE_ID)
_ncm_importacao_pipeline = _make_pipeline(NCM_IMPORTACAO_TABLE_ID)


# ──────────────────────────────────────────────────────────────────────────────
# municipio_exportacao
# ──────────────────────────────────────────────────────────────────────────────


@flow(
    name=_municipio_exportacao_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_me_comex_stat_municipio_exportacao_check_update_flow() -> None:
    _municipio_exportacao_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_me_comex_stat_municipio_exportacao_check_update_flow.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE)
)


@flow(
    name=_municipio_exportacao_pipeline.download_flow_name,
    log_prints=True,
)
def br_me_comex_stat_municipio_exportacao_download_flow(
    download_params: dict,
) -> None:
    _municipio_exportacao_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_me_comex_stat_municipio_exportacao_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_municipio_exportacao_pipeline.download_deployment = (
    br_me_comex_stat_municipio_exportacao_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# municipio_importacao
# ──────────────────────────────────────────────────────────────────────────────


@flow(
    name=_municipio_importacao_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_me_comex_stat_municipio_importacao_check_update_flow() -> None:
    _municipio_importacao_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_me_comex_stat_municipio_importacao_check_update_flow.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE)
)


@flow(
    name=_municipio_importacao_pipeline.download_flow_name,
    log_prints=True,
)
def br_me_comex_stat_municipio_importacao_download_flow(
    download_params: dict,
) -> None:
    _municipio_importacao_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_me_comex_stat_municipio_importacao_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_municipio_importacao_pipeline.download_deployment = (
    br_me_comex_stat_municipio_importacao_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# ncm_exportacao
# ──────────────────────────────────────────────────────────────────────────────


@flow(
    name=_ncm_exportacao_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_me_comex_stat_ncm_exportacao_check_update_flow() -> None:
    _ncm_exportacao_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_me_comex_stat_ncm_exportacao_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(
    name=_ncm_exportacao_pipeline.download_flow_name,
    log_prints=True,
)
def br_me_comex_stat_ncm_exportacao_download_flow(
    download_params: dict,
) -> None:
    _ncm_exportacao_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_me_comex_stat_ncm_exportacao_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_ncm_exportacao_pipeline.download_deployment = (
    br_me_comex_stat_ncm_exportacao_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# ncm_importacao
# ──────────────────────────────────────────────────────────────────────────────


@flow(
    name=_ncm_importacao_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_me_comex_stat_ncm_importacao_check_update_flow() -> None:
    _ncm_importacao_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_me_comex_stat_ncm_importacao_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(
    name=_ncm_importacao_pipeline.download_flow_name,
    log_prints=True,
)
def br_me_comex_stat_ncm_importacao_download_flow(
    download_params: dict,
) -> None:
    _ncm_importacao_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_me_comex_stat_ncm_importacao_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_ncm_importacao_pipeline.download_deployment = (
    br_me_comex_stat_ncm_importacao_download_flow.fn.__name__
)
