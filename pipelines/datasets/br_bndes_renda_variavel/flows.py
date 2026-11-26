"""Flows de br_bndes_renda_variavel — Prefect 3, modelo em estágios."""

from prefect.schedules import Cron

from pipelines.datasets.br_bndes_renda_variavel.constants import (
    DATASET_ID,
    DESEMBOLSO_TABLE_ID,
    EMPRESA_DEBENTURE_TABLE_ID,
    EMPRESA_PARTICIPACAO_ACIONARIA_TABLE_ID,
    FUNDO_PARTICIPACAO_TABLE_ID,
)
from pipelines.datasets.br_bndes_renda_variavel.tasks import (
    desembolso_extract_load_data,
    desembolso_get_latest_update,
    empresa_debenture_extract_load_data,
    empresa_debenture_get_latest_update,
    empresa_participacao_acionaria_extract_load_data,
    empresa_participacao_acionaria_get_latest_update,
    fundo_participacao_extract_load_data,
    fundo_participacao_get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import (
    CheckThenExtractLoadPipeline,
    Etapa,
    deploy_tags,
)

_empresa_participacao_acionaria_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=EMPRESA_PARTICIPACAO_ACIONARIA_TABLE_ID,
    get_latest_update=empresa_participacao_acionaria_get_latest_update,
    extract_load_data=empresa_participacao_acionaria_extract_load_data,
)


@flow(
    name=_empresa_participacao_acionaria_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_renda_variavel_empresa_participacao_acionaria_check_update() -> (
    None
):
    _empresa_participacao_acionaria_pipeline.run_check_update()


br_bndes_renda_variavel_empresa_participacao_acionaria_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, EMPRESA_PARTICIPACAO_ACIONARIA_TABLE_ID
)
br_bndes_renda_variavel_empresa_participacao_acionaria_check_update.deploy_schedules = [
    Cron("10 3 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_empresa_participacao_acionaria_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_renda_variavel_empresa_participacao_acionaria_download(
    download_params: dict,
) -> None:
    _empresa_participacao_acionaria_pipeline.run_extract_and_load(
        download_params
    )


br_bndes_renda_variavel_empresa_participacao_acionaria_download.deploy_tags = (
    deploy_tags(
        DATASET_ID,
        Etapa.EXTRACT_AND_LOAD,
        EMPRESA_PARTICIPACAO_ACIONARIA_TABLE_ID,
    )
)
_empresa_participacao_acionaria_pipeline.extract_load_deployment = (
    br_bndes_renda_variavel_empresa_participacao_acionaria_download.fn.__name__
)


_fundo_participacao_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=FUNDO_PARTICIPACAO_TABLE_ID,
    get_latest_update=fundo_participacao_get_latest_update,
    extract_load_data=fundo_participacao_extract_load_data,
)


@flow(
    name=_fundo_participacao_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_renda_variavel_fundo_participacao_check_update() -> None:
    _fundo_participacao_pipeline.run_check_update()


br_bndes_renda_variavel_fundo_participacao_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, FUNDO_PARTICIPACAO_TABLE_ID)
)
br_bndes_renda_variavel_fundo_participacao_check_update.deploy_schedules = [
    Cron("15 3 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_fundo_participacao_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_renda_variavel_fundo_participacao_download(
    download_params: dict,
) -> None:
    _fundo_participacao_pipeline.run_extract_and_load(download_params)


br_bndes_renda_variavel_fundo_participacao_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, FUNDO_PARTICIPACAO_TABLE_ID
)
_fundo_participacao_pipeline.extract_load_deployment = (
    br_bndes_renda_variavel_fundo_participacao_download.fn.__name__
)


_empresa_debenture_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=EMPRESA_DEBENTURE_TABLE_ID,
    get_latest_update=empresa_debenture_get_latest_update,
    extract_load_data=empresa_debenture_extract_load_data,
)


@flow(
    name=_empresa_debenture_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_renda_variavel_empresa_debenture_check_update() -> None:
    _empresa_debenture_pipeline.run_check_update()


br_bndes_renda_variavel_empresa_debenture_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, EMPRESA_DEBENTURE_TABLE_ID)
)
br_bndes_renda_variavel_empresa_debenture_check_update.deploy_schedules = [
    Cron("20 3 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_empresa_debenture_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_renda_variavel_empresa_debenture_download(
    download_params: dict,
) -> None:
    _empresa_debenture_pipeline.run_extract_and_load(download_params)


br_bndes_renda_variavel_empresa_debenture_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, EMPRESA_DEBENTURE_TABLE_ID
)
_empresa_debenture_pipeline.extract_load_deployment = (
    br_bndes_renda_variavel_empresa_debenture_download.fn.__name__
)


_desembolso_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=DESEMBOLSO_TABLE_ID,
    get_latest_update=desembolso_get_latest_update,
    extract_load_data=desembolso_extract_load_data,
)


@flow(
    name=_desembolso_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_renda_variavel_desembolso_check_update() -> None:
    _desembolso_pipeline.run_check_update()


br_bndes_renda_variavel_desembolso_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, DESEMBOLSO_TABLE_ID
)
br_bndes_renda_variavel_desembolso_check_update.deploy_schedules = [
    Cron("35 3 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_desembolso_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_renda_variavel_desembolso_download(download_params: dict) -> None:
    _desembolso_pipeline.run_extract_and_load(download_params)


br_bndes_renda_variavel_desembolso_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, DESEMBOLSO_TABLE_ID
)
_desembolso_pipeline.extract_load_deployment = (
    br_bndes_renda_variavel_desembolso_download.fn.__name__
)
