"""
Flows for br_rf_cnpj — Prefect 3.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_rf_cnpj.constants import (
    CHECK_UPDATE_CRON,
    DATASET_ID,
    DICIONARIO_TABLE_ID,
    EMPRESAS_TABLE_ID,
    ESTABELECIMENTOS_TABLE_ID,
    SIMPLES_TABLE_ID,
    SOCIOS_TABLE_ID,
)
from pipelines.datasets.br_rf_cnpj.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# ──────────────────────────────────────────────────────────────────────────────
# empresas
# check_update: br_rf_cnpj__empresas
# extract_and_load: br_rf_cnpj__empresas
# ──────────────────────────────────────────────────────────────────────────────

_empresas_pipeline = make_pipeline(EMPRESAS_TABLE_ID)


@flow(name=_empresas_pipeline.check_update_flow_name, log_prints=True)
def br_rf_cnpj_empresas_check_update() -> None:
    _empresas_pipeline.run_check_update()


br_rf_cnpj_empresas_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, EMPRESAS_TABLE_ID
)

br_rf_cnpj_empresas_check_update.deploy_schedules = [
    Cron(CHECK_UPDATE_CRON[EMPRESAS_TABLE_ID], timezone="America/Sao_Paulo")
]


@flow(name=_empresas_pipeline.extract_and_load_flow_name, log_prints=True)
def br_rf_cnpj_empresas_download(download_params: dict) -> None:
    _empresas_pipeline.run_extract_and_load(download_params)


br_rf_cnpj_empresas_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, EMPRESAS_TABLE_ID
)
_empresas_pipeline.extract_load_deployment = (
    br_rf_cnpj_empresas_download.fn.__name__
)

# ──────────────────────────────────────────────────────────────────────────────
# empresas
# check_update: br_rf_cnpj__empresas
# extract_and_load: br_rf_cnpj__empresas
# ──────────────────────────────────────────────────────────────────────────────

_empresas_pipeline = make_pipeline(ESTABELECIMENTOS_TABLE_ID)


@flow(name=_empresas_pipeline.check_update_flow_name, log_prints=True)
def br_rf_cnpj_empresas_check_update() -> None:
    _empresas_pipeline.run_check_update()


br_rf_cnpj_empresas_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, ESTABELECIMENTOS_TABLE_ID
)

br_rf_cnpj_empresas_check_update.deploy_schedules = [
    Cron(
        CHECK_UPDATE_CRON[ESTABELECIMENTOS_TABLE_ID],
        timezone="America/Sao_Paulo",
    )
]


@flow(name=_empresas_pipeline.extract_and_load_flow_name, log_prints=True)
def br_rf_cnpj_empresas_download(download_params: dict) -> None:
    _empresas_pipeline.run_extract_and_load(download_params)


br_rf_cnpj_empresas_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, ESTABELECIMENTOS_TABLE_ID
)
_empresas_pipeline.extract_load_deployment = (
    br_rf_cnpj_empresas_download.fn.__name__
)

## Precisa resolver isso
# # estabelecimentos: atualiza diretório de estabelecimentos
# if table_id == "estabelecimentos":
#     run_dbt(
#         dataset_id="br_bd_diretorios_brasil",
#         table_id="empresa",
#         dbt_command="run/test",
#         target=target,
#     )
#     download_data_to_gcs(
#         dataset_id="br_bd_diretorios_brasil",
#         table_id="empresa",
#     )
#     if update_metadata:
#         register_table_materialization_task(
#             dataset_id="br_bd_diretorios_brasil",
#             table_id="empresa",
#             coverage=AllBdpro(
#                 date_column=DateOnly(col="data_referencia"),
#                 date_format=DateFormat.YEAR_MD,
#             ),
#             env="prod",
#             bq_project="basedosdados",
#         )


# ──────────────────────────────────────────────────────────────────────────────
# socios
# check_update: br_rf_cnpj__socios
# extract_and_load: br_rf_cnpj__socios
# ──────────────────────────────────────────────────────────────────────────────

_socios_pipeline = make_pipeline(SOCIOS_TABLE_ID)


@flow(name=_socios_pipeline.check_update_flow_name, log_prints=True)
def br_rf_cnpj_socios_check_update() -> None:
    _socios_pipeline.run_check_update()


br_rf_cnpj_socios_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, SOCIOS_TABLE_ID
)

br_rf_cnpj_socios_check_update.deploy_schedules = [
    Cron(CHECK_UPDATE_CRON[SOCIOS_TABLE_ID], timezone="America/Sao_Paulo")
]


@flow(name=_socios_pipeline.extract_and_load_flow_name, log_prints=True)
def br_rf_cnpj_socios_download(download_params: dict) -> None:
    _socios_pipeline.run_extract_and_load(download_params)


br_rf_cnpj_socios_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, SOCIOS_TABLE_ID
)
_socios_pipeline.extract_load_deployment = (
    br_rf_cnpj_socios_download.fn.__name__
)

# ──────────────────────────────────────────────────────────────────────────────
# simples
# check_update: br_rf_cnpj__simples
# extract_and_load: br_rf_cnpj__simples
# ──────────────────────────────────────────────────────────────────────────────

_simples_pipeline = make_pipeline(SIMPLES_TABLE_ID)


@flow(name=_simples_pipeline.check_update_flow_name, log_prints=True)
def br_rf_cnpj_simples_check_update() -> None:
    _simples_pipeline.run_check_update()


br_rf_cnpj_simples_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, SIMPLES_TABLE_ID
)

br_rf_cnpj_simples_check_update.deploy_schedules = [
    Cron(CHECK_UPDATE_CRON[SIMPLES_TABLE_ID], timezone="America/Sao_Paulo")
]


@flow(name=_simples_pipeline.extract_and_load_flow_name, log_prints=True)
def br_rf_cnpj_simples_download(download_params: dict) -> None:
    _simples_pipeline.run_extract_and_load(download_params)


br_rf_cnpj_simples_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, SIMPLES_TABLE_ID
)
_simples_pipeline.extract_load_deployment = (
    br_rf_cnpj_simples_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# dicionario
# check_update: br_rf_cnpj__dicionario
# extract_and_load: br_rf_cnpj__dicionario
# ──────────────────────────────────────────────────────────────────────────────

_dicionario_pipeline = make_pipeline(DICIONARIO_TABLE_ID)


@flow(name=_dicionario_pipeline.check_update_flow_name, log_prints=True)
def br_rf_cnpj_dicionario_check_update() -> None:
    _dicionario_pipeline.run_check_update()


br_rf_cnpj_dicionario_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, DICIONARIO_TABLE_ID
)

br_rf_cnpj_dicionario_check_update.deploy_schedules = [
    Cron(CHECK_UPDATE_CRON[DICIONARIO_TABLE_ID], timezone="America/Sao_Paulo")
]


@flow(name=_dicionario_pipeline.extract_and_load_flow_name, log_prints=True)
def br_rf_cnpj_dicionario_download(download_params: dict) -> None:
    _dicionario_pipeline.run_extract_and_load(download_params)


br_rf_cnpj_dicionario_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, DICIONARIO_TABLE_ID
)
_dicionario_pipeline.extract_load_deployment = (
    br_rf_cnpj_dicionario_download.fn.__name__
)
