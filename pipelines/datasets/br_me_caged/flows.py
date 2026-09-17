"""
Flows para br_me_caged — Prefect 3.

Migrado por completo pro pipeline orientado a eventos (issue #1867):
check_update -> download -> mat_test, uma dupla de flows por tabela.
Lógica específica do dataset mora em `tasks.py`, constantes em
`constants.py` — aqui só a fiação (`CheckThenDownloadPipeline` + `@flow`).

O antigo `_caged_flow`/`_run_me_caged` monolítico foi removido deste
arquivo — a lógica de baixo nível que ele usava continua em
`pipelines/crawler/me_caged/` (reaproveitada por `tasks.py`).
"""

from prefect import flow

from pipelines.datasets.br_me_caged.constants import (
    DATASET_ID,
    MICRODADOS_MOVIMENTACAO_EXCLUIDA_TABLE_ID,
    MICRODADOS_MOVIMENTACAO_FORA_PRAZO_TABLE_ID,
    MICRODADOS_MOVIMENTACAO_TABLE_ID,
)
from pipelines.datasets.br_me_caged.tasks import make_pipeline
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# ──────────────────────────────────────────────────────────────────────────────
# microdados_movimentacao
# check_update: br_me_caged__microdados_movimentacao
# download: br_me_caged__microdados_movimentacao
# ──────────────────────────────────────────────────────────────────────────────

_microdados_movimentacao_pipeline = make_pipeline(
    MICRODADOS_MOVIMENTACAO_TABLE_ID
)


@flow(
    name=_microdados_movimentacao_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_me_caged_microdados_movimentacao_check_update() -> None:
    _microdados_movimentacao_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_me_caged_microdados_movimentacao_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(
    name=_microdados_movimentacao_pipeline.download_flow_name,
    log_prints=True,
)
def br_me_caged_microdados_movimentacao_download(
    download_params: dict,
) -> None:
    _microdados_movimentacao_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_me_caged_microdados_movimentacao_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_microdados_movimentacao_pipeline.download_deployment = (
    br_me_caged_microdados_movimentacao_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# microdados_movimentacao_fora_prazo
# check_update: br_me_caged__microdados_movimentacao_fora_prazo
# download: br_me_caged__microdados_movimentacao_fora_prazo
# ──────────────────────────────────────────────────────────────────────────────

_microdados_movimentacao_fora_prazo_pipeline = make_pipeline(
    MICRODADOS_MOVIMENTACAO_FORA_PRAZO_TABLE_ID
)


@flow(
    name=_microdados_movimentacao_fora_prazo_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_me_caged_microdados_movimentacao_fora_prazo_check_update() -> None:
    _microdados_movimentacao_fora_prazo_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_me_caged_microdados_movimentacao_fora_prazo_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE)
)


@flow(
    name=_microdados_movimentacao_fora_prazo_pipeline.download_flow_name,
    log_prints=True,
)
def br_me_caged_microdados_movimentacao_fora_prazo_download(
    download_params: dict,
) -> None:
    _microdados_movimentacao_fora_prazo_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_me_caged_microdados_movimentacao_fora_prazo_download.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.DOWNLOAD)
)
_microdados_movimentacao_fora_prazo_pipeline.download_deployment = (
    br_me_caged_microdados_movimentacao_fora_prazo_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# microdados_movimentacao_excluida
# check_update: br_me_caged__microdados_movimentacao_excluida
# download: br_me_caged__microdados_movimentacao_excluida
# ──────────────────────────────────────────────────────────────────────────────

_microdados_movimentacao_excluida_pipeline = make_pipeline(
    MICRODADOS_MOVIMENTACAO_EXCLUIDA_TABLE_ID
)


@flow(
    name=_microdados_movimentacao_excluida_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_me_caged_microdados_movimentacao_excluida_check_update() -> None:
    _microdados_movimentacao_excluida_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_me_caged_microdados_movimentacao_excluida_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE)
)


@flow(
    name=_microdados_movimentacao_excluida_pipeline.download_flow_name,
    log_prints=True,
)
def br_me_caged_microdados_movimentacao_excluida_download(
    download_params: dict,
) -> None:
    _microdados_movimentacao_excluida_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_me_caged_microdados_movimentacao_excluida_download.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.DOWNLOAD)
)
_microdados_movimentacao_excluida_pipeline.download_deployment = (
    br_me_caged_microdados_movimentacao_excluida_download.fn.__name__
)
