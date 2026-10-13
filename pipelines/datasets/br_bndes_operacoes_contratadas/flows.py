"""Flows de br_bndes_operacoes_contratadas — Prefect 3, modelo em estágios."""

from prefect.schedules import Cron

from pipelines.datasets.br_bndes_operacoes_contratadas.constants import (
    constants_administracao_publica,
    constants_exportacao_bens,
    constants_exportacao_servicos,
    constants_indiretas_automaticas,
    constants_nao_automaticas,
    constants_pre_embarque,
)
from pipelines.datasets.br_bndes_operacoes_contratadas.tasks import (
    administracao_publica_extract_load_data,
    administracao_publica_get_latest_update,
    exportacao_bens_extract_load_data,
    exportacao_bens_get_latest_update,
    exportacao_servicos_extract_load_data,
    exportacao_servicos_get_latest_update,
    indiretas_automaticas_extract_load_data,
    indiretas_automaticas_get_latest_update,
    nao_automaticas_extract_load_data,
    nao_automaticas_get_latest_update,
    pre_embarque_extract_load_data,
    pre_embarque_get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import (
    CheckThenExtractLoadPipeline,
    Etapa,
    deploy_tags,
)

_indiretas_automaticas_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=constants_indiretas_automaticas.DATASET_ID.value,
    table_id=constants_indiretas_automaticas.TABLE_ID.value,
    get_latest_update=indiretas_automaticas_get_latest_update,
    extract_load_data=indiretas_automaticas_extract_load_data,
)


@flow(
    name=_indiretas_automaticas_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_operacoes_contratadas_operacoes_indiretas_automaticas_check_update() -> (
    None
):
    _indiretas_automaticas_pipeline.run_check_update()


br_bndes_operacoes_contratadas_operacoes_indiretas_automaticas_check_update.deploy_tags = deploy_tags(
    constants_indiretas_automaticas.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_indiretas_automaticas.TABLE_ID.value,
)
br_bndes_operacoes_contratadas_operacoes_indiretas_automaticas_check_update.deploy_schedules = [
    Cron("5 6 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_indiretas_automaticas_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_operacoes_contratadas_operacoes_indiretas_automaticas_download(
    download_params: dict,
) -> None:
    _indiretas_automaticas_pipeline.run_extract_and_load(download_params)


br_bndes_operacoes_contratadas_operacoes_indiretas_automaticas_download.deploy_tags = deploy_tags(
    constants_indiretas_automaticas.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_indiretas_automaticas.TABLE_ID.value,
)
_indiretas_automaticas_pipeline.extract_load_deployment = br_bndes_operacoes_contratadas_operacoes_indiretas_automaticas_download.fn.__name__


_nao_automaticas_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=constants_nao_automaticas.DATASET_ID.value,
    table_id=constants_nao_automaticas.TABLE_ID.value,
    get_latest_update=nao_automaticas_get_latest_update,
    extract_load_data=nao_automaticas_extract_load_data,
)


@flow(
    name=_nao_automaticas_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_operacoes_contratadas_operacoes_nao_automaticas_check_update() -> (
    None
):
    _nao_automaticas_pipeline.run_check_update()


br_bndes_operacoes_contratadas_operacoes_nao_automaticas_check_update.deploy_tags = deploy_tags(
    constants_nao_automaticas.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_nao_automaticas.TABLE_ID.value,
)
br_bndes_operacoes_contratadas_operacoes_nao_automaticas_check_update.deploy_schedules = [
    Cron("10 6 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_nao_automaticas_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_operacoes_contratadas_operacoes_nao_automaticas_download(
    download_params: dict,
) -> None:
    _nao_automaticas_pipeline.run_extract_and_load(download_params)


br_bndes_operacoes_contratadas_operacoes_nao_automaticas_download.deploy_tags = deploy_tags(
    constants_nao_automaticas.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_nao_automaticas.TABLE_ID.value,
)
_nao_automaticas_pipeline.extract_load_deployment = br_bndes_operacoes_contratadas_operacoes_nao_automaticas_download.fn.__name__


_administracao_publica_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=constants_administracao_publica.DATASET_ID.value,
    table_id=constants_administracao_publica.TABLE_ID.value,
    get_latest_update=administracao_publica_get_latest_update,
    extract_load_data=administracao_publica_extract_load_data,
)


@flow(
    name=_administracao_publica_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_operacoes_contratadas_operacoes_administracao_publica_check_update() -> (
    None
):
    _administracao_publica_pipeline.run_check_update()


br_bndes_operacoes_contratadas_operacoes_administracao_publica_check_update.deploy_tags = deploy_tags(
    constants_administracao_publica.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_administracao_publica.TABLE_ID.value,
)
br_bndes_operacoes_contratadas_operacoes_administracao_publica_check_update.deploy_schedules = [
    Cron("20 6 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_administracao_publica_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_operacoes_contratadas_operacoes_administracao_publica_download(
    download_params: dict,
) -> None:
    _administracao_publica_pipeline.run_extract_and_load(download_params)


br_bndes_operacoes_contratadas_operacoes_administracao_publica_download.deploy_tags = deploy_tags(
    constants_administracao_publica.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_administracao_publica.TABLE_ID.value,
)
_administracao_publica_pipeline.extract_load_deployment = br_bndes_operacoes_contratadas_operacoes_administracao_publica_download.fn.__name__


_exportacao_bens_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=constants_exportacao_bens.DATASET_ID.value,
    table_id=constants_exportacao_bens.TABLE_ID.value,
    get_latest_update=exportacao_bens_get_latest_update,
    extract_load_data=exportacao_bens_extract_load_data,
)


@flow(
    name=_exportacao_bens_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_operacoes_contratadas_operacoes_exportacao_bens_check_update() -> (
    None
):
    _exportacao_bens_pipeline.run_check_update()


br_bndes_operacoes_contratadas_operacoes_exportacao_bens_check_update.deploy_tags = deploy_tags(
    constants_exportacao_bens.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_exportacao_bens.TABLE_ID.value,
)
br_bndes_operacoes_contratadas_operacoes_exportacao_bens_check_update.deploy_schedules = [
    Cron("25 6 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_exportacao_bens_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_operacoes_contratadas_operacoes_exportacao_bens_download(
    download_params: dict,
) -> None:
    _exportacao_bens_pipeline.run_extract_and_load(download_params)


br_bndes_operacoes_contratadas_operacoes_exportacao_bens_download.deploy_tags = deploy_tags(
    constants_exportacao_bens.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_exportacao_bens.TABLE_ID.value,
)
_exportacao_bens_pipeline.extract_load_deployment = br_bndes_operacoes_contratadas_operacoes_exportacao_bens_download.fn.__name__


_exportacao_servicos_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=constants_exportacao_servicos.DATASET_ID.value,
    table_id=constants_exportacao_servicos.TABLE_ID.value,
    get_latest_update=exportacao_servicos_get_latest_update,
    extract_load_data=exportacao_servicos_extract_load_data,
)


@flow(
    name=_exportacao_servicos_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_operacoes_contratadas_operacoes_exportacao_servicos_check_update() -> (
    None
):
    _exportacao_servicos_pipeline.run_check_update()


br_bndes_operacoes_contratadas_operacoes_exportacao_servicos_check_update.deploy_tags = deploy_tags(
    constants_exportacao_servicos.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_exportacao_servicos.TABLE_ID.value,
)


@flow(
    name=_exportacao_servicos_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_operacoes_contratadas_operacoes_exportacao_servicos_download(
    download_params: dict,
) -> None:
    _exportacao_servicos_pipeline.run_extract_and_load(download_params)


br_bndes_operacoes_contratadas_operacoes_exportacao_servicos_download.deploy_tags = deploy_tags(
    constants_exportacao_servicos.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_exportacao_servicos.TABLE_ID.value,
)
_exportacao_servicos_pipeline.extract_load_deployment = br_bndes_operacoes_contratadas_operacoes_exportacao_servicos_download.fn.__name__


_pre_embarque_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=constants_pre_embarque.DATASET_ID.value,
    table_id=constants_pre_embarque.TABLE_ID.value,
    get_latest_update=pre_embarque_get_latest_update,
    extract_load_data=pre_embarque_extract_load_data,
)


@flow(
    name=_pre_embarque_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_operacoes_contratadas_operacoes_pre_embarque_check_update() -> (
    None
):
    _pre_embarque_pipeline.run_check_update()


br_bndes_operacoes_contratadas_operacoes_pre_embarque_check_update.deploy_tags = deploy_tags(
    constants_pre_embarque.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_pre_embarque.TABLE_ID.value,
)
br_bndes_operacoes_contratadas_operacoes_pre_embarque_check_update.deploy_schedules = [
    Cron("25 2 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_pre_embarque_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_operacoes_contratadas_operacoes_pre_embarque_download(
    download_params: dict,
) -> None:
    _pre_embarque_pipeline.run_extract_and_load(download_params)


br_bndes_operacoes_contratadas_operacoes_pre_embarque_download.deploy_tags = (
    deploy_tags(
        constants_pre_embarque.DATASET_ID.value,
        Etapa.EXTRACT_AND_LOAD,
        constants_pre_embarque.TABLE_ID.value,
    )
)
_pre_embarque_pipeline.extract_load_deployment = (
    br_bndes_operacoes_contratadas_operacoes_pre_embarque_download.fn.__name__
)
