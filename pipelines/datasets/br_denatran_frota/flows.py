"""
Flows para br_denatran_frota — Prefect 3.

Migrado por completo pro pipeline orientado a eventos (issue #1867):
check_update -> download -> mat_test, uma dupla de flows por tabela.
Lógica específica do dataset mora em `tasks.py`, constantes em
`constants.py` — aqui só a fiação (`CheckThenDownloadPipeline` + `@flow`).
"""

from prefect import flow

from pipelines.datasets.br_denatran_frota.constants import (
    DATASET_ID,
    MUNICIPIO_TIPO_TABLE_ID,
    UF_TIPO_TABLE_ID,
)
from pipelines.datasets.br_denatran_frota.tasks import (
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
        # Mesma granularidade do flow antigo (`_run_denatran`, que já
        # compara coverage com date_format="%Y-%m" — o dado é mensal).
        date_format="%Y-%m",
    )


_uf_tipo_pipeline = _make_pipeline(UF_TIPO_TABLE_ID)
_municipio_tipo_pipeline = _make_pipeline(MUNICIPIO_TIPO_TABLE_ID)


# ──────────────────────────────────────────────────────────────────────────────
# uf_tipo
# ──────────────────────────────────────────────────────────────────────────────


@flow(name=_uf_tipo_pipeline.check_update_flow_name, log_prints=True)
def br_denatran_frota_uf_tipo_check_update_flow() -> None:
    _uf_tipo_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_denatran_frota_uf_tipo_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_uf_tipo_pipeline.download_flow_name, log_prints=True)
def br_denatran_frota_uf_tipo_download_flow(download_params: dict) -> None:
    _uf_tipo_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_denatran_frota_uf_tipo_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_uf_tipo_pipeline.download_deployment = (
    br_denatran_frota_uf_tipo_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# municipio_tipo
# ──────────────────────────────────────────────────────────────────────────────


@flow(name=_municipio_tipo_pipeline.check_update_flow_name, log_prints=True)
def br_denatran_frota_municipio_tipo_check_update_flow() -> None:
    _municipio_tipo_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_denatran_frota_municipio_tipo_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_municipio_tipo_pipeline.download_flow_name, log_prints=True)
def br_denatran_frota_municipio_tipo_download_flow(
    download_params: dict,
) -> None:
    _municipio_tipo_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_denatran_frota_municipio_tipo_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_municipio_tipo_pipeline.download_deployment = (
    br_denatran_frota_municipio_tipo_download_flow.fn.__name__
)
