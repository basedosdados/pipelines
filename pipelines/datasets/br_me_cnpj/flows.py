"""
Flows para br_me_cnpj — Prefect 3.

Migrado por completo pro pipeline orientado a eventos (issue #1867):
check_update -> download -> mat_test, uma dupla de flows por tabela.
Lógica específica do dataset mora em `tasks.py`, constantes em
`constants.py` — aqui só a fiação (`CheckThenDownloadPipeline` + `@flow`).

O antigo `_me_cnpj_flow`/`_run_me_cnpj` monolítico (`crawler/me_cnpj/`)
não tem outro consumidor além deste arquivo — não removido do
`crawler/`, só não é mais referenciado aqui.

Gap conhecido (não replicado ainda): o flow antigo, só pra `estabelecimentos`,
também rodava `run_dbt(br_bd_diretorios_brasil.empresa)` +
`download_data_to_gcs` + `register_table_materialization_task` pra um
dataset diferente (`br_bd_diretorios_brasil`) logo após materializar
`estabelecimentos`. O `mat_test_flow` genérico não tem esse gancho —
fica pendente até `stage_dispatch.py` ganhar um jeito de anexar
pós-processamento extra por tabela.
"""

from prefect import flow

from pipelines.datasets.br_me_cnpj.constants import (
    DATASET_ID,
    EMPRESAS_TABLE_ID,
    ESTABELECIMENTOS_TABLE_ID,
    HEAVY_DOWNLOAD_JOB_VARIABLES,
    SIMPLES_COMPARE_AGAINST,
    SIMPLES_TABLE_ID,
    SOCIOS_TABLE_ID,
)
from pipelines.datasets.br_me_cnpj.tasks import (
    make_check_for_update,
    make_download_data,
)
from pipelines.utils.stage_dispatch import (
    CheckThenDownloadPipeline,
    Etapa,
    deploy_tags,
)


def _make_pipeline(
    table_id: str, compare_against: str = "coverage"
) -> CheckThenDownloadPipeline:
    return CheckThenDownloadPipeline(
        dataset_id=DATASET_ID,
        table_id=table_id,
        check_for_update=make_check_for_update(table_id),
        download_data=make_download_data(table_id),
        # Mesma granularidade do flow antigo (`_run_me_cnpj`, que já
        # compara coverage com date_format="%Y-%m").
        date_format="%Y-%m",
        compare_against=compare_against,
    )


_empresas_pipeline = _make_pipeline(EMPRESAS_TABLE_ID)
_socios_pipeline = _make_pipeline(SOCIOS_TABLE_ID)
_estabelecimentos_pipeline = _make_pipeline(ESTABELECIMENTOS_TABLE_ID)
_simples_pipeline = _make_pipeline(
    SIMPLES_TABLE_ID, compare_against=SIMPLES_COMPARE_AGAINST
)


# ──────────────────────────────────────────────────────────────────────────────
# empresas
# ──────────────────────────────────────────────────────────────────────────────


@flow(name=_empresas_pipeline.check_update_flow_name, log_prints=True)
def br_me_cnpj_empresas_check_update_flow() -> None:
    _empresas_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_me_cnpj_empresas_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_empresas_pipeline.download_flow_name, log_prints=True)
def br_me_cnpj_empresas_download_flow(download_params: dict) -> None:
    _empresas_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_me_cnpj_empresas_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
# pyrefly: ignore [missing-attribute]
br_me_cnpj_empresas_download_flow.job_variables = HEAVY_DOWNLOAD_JOB_VARIABLES
_empresas_pipeline.download_deployment = (
    br_me_cnpj_empresas_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# socios
# ──────────────────────────────────────────────────────────────────────────────


@flow(name=_socios_pipeline.check_update_flow_name, log_prints=True)
def br_me_cnpj_socios_check_update_flow() -> None:
    _socios_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_me_cnpj_socios_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_socios_pipeline.download_flow_name, log_prints=True)
def br_me_cnpj_socios_download_flow(download_params: dict) -> None:
    _socios_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_me_cnpj_socios_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
# pyrefly: ignore [missing-attribute]
br_me_cnpj_socios_download_flow.job_variables = HEAVY_DOWNLOAD_JOB_VARIABLES
_socios_pipeline.download_deployment = (
    br_me_cnpj_socios_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# estabelecimentos
# ──────────────────────────────────────────────────────────────────────────────


@flow(name=_estabelecimentos_pipeline.check_update_flow_name, log_prints=True)
def br_me_cnpj_estabelecimentos_check_update_flow() -> None:
    _estabelecimentos_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_me_cnpj_estabelecimentos_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_estabelecimentos_pipeline.download_flow_name, log_prints=True)
def br_me_cnpj_estabelecimentos_download_flow(
    download_params: dict,
) -> None:
    _estabelecimentos_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_me_cnpj_estabelecimentos_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
# pyrefly: ignore [missing-attribute]
br_me_cnpj_estabelecimentos_download_flow.job_variables = (
    HEAVY_DOWNLOAD_JOB_VARIABLES
)
_estabelecimentos_pipeline.download_deployment = (
    br_me_cnpj_estabelecimentos_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# simples — NonHistorical, compare_against="table_update" (ver constants.py)
# ──────────────────────────────────────────────────────────────────────────────


@flow(name=_simples_pipeline.check_update_flow_name, log_prints=True)
def br_me_cnpj_simples_check_update_flow() -> None:
    _simples_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_me_cnpj_simples_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_simples_pipeline.download_flow_name, log_prints=True)
def br_me_cnpj_simples_download_flow(download_params: dict) -> None:
    _simples_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_me_cnpj_simples_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_simples_pipeline.download_deployment = (
    br_me_cnpj_simples_download_flow.fn.__name__
)
