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
from pipelines.datasets.br_me_caged.tasks import (
    br_me_caged_check_for_update,
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
        # Mesma checagem (leve, via FTP) pras 3 tabelas — ver banner em
        # tasks.py.
        check_for_update=br_me_caged_check_for_update,
        download_data=make_download_data(table_id),
        # Mesma granularidade do flow antigo (`_run_me_caged`, que já
        # compara coverage com date_format="%Y-%m" — o dado é mensal, sem dia).
        date_format="%Y-%m",
    )


_microdados_movimentacao_pipeline = _make_pipeline(
    MICRODADOS_MOVIMENTACAO_TABLE_ID
)
_microdados_movimentacao_fora_prazo_pipeline = _make_pipeline(
    MICRODADOS_MOVIMENTACAO_FORA_PRAZO_TABLE_ID
)
_microdados_movimentacao_excluida_pipeline = _make_pipeline(
    MICRODADOS_MOVIMENTACAO_EXCLUIDA_TABLE_ID
)


# ──────────────────────────────────────────────────────────────────────────────
# microdados_movimentacao
# ──────────────────────────────────────────────────────────────────────────────


@flow(
    name=_microdados_movimentacao_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_me_caged_microdados_movimentacao_check_update_flow() -> None:
    _microdados_movimentacao_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_me_caged_microdados_movimentacao_check_update_flow.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE)
)


@flow(
    name=_microdados_movimentacao_pipeline.download_flow_name,
    log_prints=True,
)
def br_me_caged_microdados_movimentacao_download_flow(
    download_params: dict,
) -> None:
    _microdados_movimentacao_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_me_caged_microdados_movimentacao_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_microdados_movimentacao_pipeline.download_deployment = (
    br_me_caged_microdados_movimentacao_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# microdados_movimentacao_fora_prazo
# ──────────────────────────────────────────────────────────────────────────────


@flow(
    name=_microdados_movimentacao_fora_prazo_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_me_caged_microdados_movimentacao_fora_prazo_check_update_flow() -> None:
    _microdados_movimentacao_fora_prazo_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_me_caged_microdados_movimentacao_fora_prazo_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(
    name=_microdados_movimentacao_fora_prazo_pipeline.download_flow_name,
    log_prints=True,
)
def br_me_caged_microdados_movimentacao_fora_prazo_download_flow(
    download_params: dict,
) -> None:
    _microdados_movimentacao_fora_prazo_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_me_caged_microdados_movimentacao_fora_prazo_download_flow.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.DOWNLOAD)
)
_microdados_movimentacao_fora_prazo_pipeline.download_deployment = (
    br_me_caged_microdados_movimentacao_fora_prazo_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# microdados_movimentacao_excluida
# ──────────────────────────────────────────────────────────────────────────────


@flow(
    name=_microdados_movimentacao_excluida_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_me_caged_microdados_movimentacao_excluida_check_update_flow() -> None:
    _microdados_movimentacao_excluida_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_me_caged_microdados_movimentacao_excluida_check_update_flow.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE)
)


@flow(
    name=_microdados_movimentacao_excluida_pipeline.download_flow_name,
    log_prints=True,
)
def br_me_caged_microdados_movimentacao_excluida_download_flow(
    download_params: dict,
) -> None:
    _microdados_movimentacao_excluida_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_me_caged_microdados_movimentacao_excluida_download_flow.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.DOWNLOAD)
)
_microdados_movimentacao_excluida_pipeline.download_deployment = (
    br_me_caged_microdados_movimentacao_excluida_download_flow.fn.__name__
)
