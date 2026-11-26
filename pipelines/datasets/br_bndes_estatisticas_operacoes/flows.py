"""Flows de br_bndes_estatisticas_operacoes — Prefect 3, modelo em estágios."""

from prefect.schedules import Cron

from pipelines.datasets.br_bndes_estatisticas_operacoes.constants import (
    DATASET_ID,
    EXIM_APROVACAO_DOLAR_TABLE_ID,
    EXIM_APROVACAO_TABLE_ID,
    EXIM_CONSULTA_DOLAR_TABLE_ID,
    EXIM_CONSULTA_TABLE_ID,
    EXIM_SETOR_CNAE_APROVACAO_DOLAR_TABLE_ID,
    EXIM_SETOR_CNAE_CONSULTA_DOLAR_TABLE_ID,
    EXIM_SUBSETOR_CNAE_APROVACAO_DOLAR_TABLE_ID,
    EXIM_SUBSETOR_CNAE_CONSULTA_DOLAR_TABLE_ID,
    PORTE_APROVACAO_TABLE_ID,
    PORTE_CONSULTA_TABLE_ID,
    PRODUTO_DIRETA_APROVACAO_DOLAR_TABLE_ID,
    PRODUTO_DIRETA_APROVACAO_TABLE_ID,
    PRODUTO_DIRETA_CONSULTA_DOLAR_TABLE_ID,
    PRODUTO_DIRETA_CONSULTA_TABLE_ID,
    PRODUTO_INDIRETA_APROVACAO_DOLAR_TABLE_ID,
    PRODUTO_INDIRETA_APROVACAO_TABLE_ID,
    PRODUTO_INDIRETA_CONSULTA_DOLAR_TABLE_ID,
    PRODUTO_INDIRETA_CONSULTA_TABLE_ID,
    SETOR_BNDES_APROVACAO_TABLE_ID,
    SETOR_BNDES_CONSULTA_TABLE_ID,
    SETOR_CNAE_APROVACAO_TABLE_ID,
    SETOR_CNAE_CONSULTA_TABLE_ID,
    SUBSETOR_BNDES_APROVACAO_TABLE_ID,
    SUBSETOR_BNDES_CONSULTA_TABLE_ID,
    SUBSETOR_CNAE_APROVACAO_TABLE_ID,
    SUBSETOR_CNAE_CONSULTA_TABLE_ID,
    UF_APROVACAO_TABLE_ID,
    UF_CONSULTA_TABLE_ID,
)
from pipelines.datasets.br_bndes_estatisticas_operacoes.tasks import (
    exim_aprovacao_dolar_extract_load_data,
    exim_aprovacao_dolar_get_latest_update,
    exim_aprovacao_extract_load_data,
    exim_aprovacao_get_latest_update,
    exim_consulta_dolar_extract_load_data,
    exim_consulta_dolar_get_latest_update,
    exim_consulta_extract_load_data,
    exim_consulta_get_latest_update,
    exim_setor_cnae_aprovacao_dolar_extract_load_data,
    exim_setor_cnae_aprovacao_dolar_get_latest_update,
    exim_setor_cnae_consulta_dolar_extract_load_data,
    exim_setor_cnae_consulta_dolar_get_latest_update,
    exim_subsetor_cnae_aprovacao_dolar_extract_load_data,
    exim_subsetor_cnae_aprovacao_dolar_get_latest_update,
    exim_subsetor_cnae_consulta_dolar_extract_load_data,
    exim_subsetor_cnae_consulta_dolar_get_latest_update,
    porte_aprovacao_extract_load_data,
    porte_aprovacao_get_latest_update,
    porte_consulta_extract_load_data,
    porte_consulta_get_latest_update,
    produto_direta_aprovacao_dolar_extract_load_data,
    produto_direta_aprovacao_dolar_get_latest_update,
    produto_direta_aprovacao_extract_load_data,
    produto_direta_aprovacao_get_latest_update,
    produto_direta_consulta_dolar_extract_load_data,
    produto_direta_consulta_dolar_get_latest_update,
    produto_direta_consulta_extract_load_data,
    produto_direta_consulta_get_latest_update,
    produto_indireta_aprovacao_dolar_extract_load_data,
    produto_indireta_aprovacao_dolar_get_latest_update,
    produto_indireta_aprovacao_extract_load_data,
    produto_indireta_aprovacao_get_latest_update,
    produto_indireta_consulta_dolar_extract_load_data,
    produto_indireta_consulta_dolar_get_latest_update,
    produto_indireta_consulta_extract_load_data,
    produto_indireta_consulta_get_latest_update,
    setor_bndes_aprovacao_extract_load_data,
    setor_bndes_aprovacao_get_latest_update,
    setor_bndes_consulta_extract_load_data,
    setor_bndes_consulta_get_latest_update,
    setor_cnae_aprovacao_extract_load_data,
    setor_cnae_aprovacao_get_latest_update,
    setor_cnae_consulta_extract_load_data,
    setor_cnae_consulta_get_latest_update,
    subsetor_bndes_aprovacao_extract_load_data,
    subsetor_bndes_aprovacao_get_latest_update,
    subsetor_bndes_consulta_extract_load_data,
    subsetor_bndes_consulta_get_latest_update,
    subsetor_cnae_aprovacao_extract_load_data,
    subsetor_cnae_aprovacao_get_latest_update,
    subsetor_cnae_consulta_extract_load_data,
    subsetor_cnae_consulta_get_latest_update,
    uf_aprovacao_extract_load_data,
    uf_aprovacao_get_latest_update,
    uf_consulta_extract_load_data,
    uf_consulta_get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import (
    CheckThenExtractLoadPipeline,
    Etapa,
    deploy_tags,
)

_produto_direta_aprovacao_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=PRODUTO_DIRETA_APROVACAO_TABLE_ID,
    get_latest_update=produto_direta_aprovacao_get_latest_update,
    extract_load_data=produto_direta_aprovacao_extract_load_data,
)


@flow(
    name=_produto_direta_aprovacao_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_produto_direta_aprovacao_check_update() -> (
    None
):
    _produto_direta_aprovacao_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_produto_direta_aprovacao_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, PRODUTO_DIRETA_APROVACAO_TABLE_ID
)
br_bndes_estatisticas_operacoes_produto_direta_aprovacao_check_update.deploy_schedules = [
    Cron("5 0 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_produto_direta_aprovacao_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_produto_direta_aprovacao_download(
    download_params: dict,
) -> None:
    _produto_direta_aprovacao_pipeline.run_extract_and_load(download_params)


br_bndes_estatisticas_operacoes_produto_direta_aprovacao_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, PRODUTO_DIRETA_APROVACAO_TABLE_ID
)
_produto_direta_aprovacao_pipeline.extract_load_deployment = br_bndes_estatisticas_operacoes_produto_direta_aprovacao_download.fn.__name__


_produto_indireta_aprovacao_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=PRODUTO_INDIRETA_APROVACAO_TABLE_ID,
    get_latest_update=produto_indireta_aprovacao_get_latest_update,
    extract_load_data=produto_indireta_aprovacao_extract_load_data,
)


@flow(
    name=_produto_indireta_aprovacao_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_produto_indireta_aprovacao_check_update() -> (
    None
):
    _produto_indireta_aprovacao_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_produto_indireta_aprovacao_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, PRODUTO_INDIRETA_APROVACAO_TABLE_ID
)
br_bndes_estatisticas_operacoes_produto_indireta_aprovacao_check_update.deploy_schedules = [
    Cron("10 0 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_produto_indireta_aprovacao_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_produto_indireta_aprovacao_download(
    download_params: dict,
) -> None:
    _produto_indireta_aprovacao_pipeline.run_extract_and_load(download_params)


br_bndes_estatisticas_operacoes_produto_indireta_aprovacao_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, PRODUTO_INDIRETA_APROVACAO_TABLE_ID
)
_produto_indireta_aprovacao_pipeline.extract_load_deployment = br_bndes_estatisticas_operacoes_produto_indireta_aprovacao_download.fn.__name__


_porte_aprovacao_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=PORTE_APROVACAO_TABLE_ID,
    get_latest_update=porte_aprovacao_get_latest_update,
    extract_load_data=porte_aprovacao_extract_load_data,
)


@flow(name=_porte_aprovacao_pipeline.check_update_flow_name, log_prints=True)
def br_bndes_estatisticas_operacoes_porte_aprovacao_check_update() -> None:
    _porte_aprovacao_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_porte_aprovacao_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, PORTE_APROVACAO_TABLE_ID)
)
br_bndes_estatisticas_operacoes_porte_aprovacao_check_update.deploy_schedules = [
    Cron("15 0 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_porte_aprovacao_pipeline.extract_and_load_flow_name, log_prints=True
)
def br_bndes_estatisticas_operacoes_porte_aprovacao_download(
    download_params: dict,
) -> None:
    _porte_aprovacao_pipeline.run_extract_and_load(download_params)


br_bndes_estatisticas_operacoes_porte_aprovacao_download.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.EXTRACT_AND_LOAD, PORTE_APROVACAO_TABLE_ID)
)
_porte_aprovacao_pipeline.extract_load_deployment = (
    br_bndes_estatisticas_operacoes_porte_aprovacao_download.fn.__name__
)


_uf_aprovacao_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=UF_APROVACAO_TABLE_ID,
    get_latest_update=uf_aprovacao_get_latest_update,
    extract_load_data=uf_aprovacao_extract_load_data,
)


@flow(name=_uf_aprovacao_pipeline.check_update_flow_name, log_prints=True)
def br_bndes_estatisticas_operacoes_uf_aprovacao_check_update() -> None:
    _uf_aprovacao_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_uf_aprovacao_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, UF_APROVACAO_TABLE_ID)
)
br_bndes_estatisticas_operacoes_uf_aprovacao_check_update.deploy_schedules = [
    Cron("20 0 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_uf_aprovacao_pipeline.extract_and_load_flow_name, log_prints=True)
def br_bndes_estatisticas_operacoes_uf_aprovacao_download(
    download_params: dict,
) -> None:
    _uf_aprovacao_pipeline.run_extract_and_load(download_params)


br_bndes_estatisticas_operacoes_uf_aprovacao_download.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.EXTRACT_AND_LOAD, UF_APROVACAO_TABLE_ID)
)
_uf_aprovacao_pipeline.extract_load_deployment = (
    br_bndes_estatisticas_operacoes_uf_aprovacao_download.fn.__name__
)


_setor_cnae_aprovacao_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=SETOR_CNAE_APROVACAO_TABLE_ID,
    get_latest_update=setor_cnae_aprovacao_get_latest_update,
    extract_load_data=setor_cnae_aprovacao_extract_load_data,
)


@flow(
    name=_setor_cnae_aprovacao_pipeline.check_update_flow_name, log_prints=True
)
def br_bndes_estatisticas_operacoes_setor_cnae_aprovacao_check_update() -> (
    None
):
    _setor_cnae_aprovacao_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_setor_cnae_aprovacao_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, SETOR_CNAE_APROVACAO_TABLE_ID
)
br_bndes_estatisticas_operacoes_setor_cnae_aprovacao_check_update.deploy_schedules = [
    Cron("25 0 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_setor_cnae_aprovacao_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_setor_cnae_aprovacao_download(
    download_params: dict,
) -> None:
    _setor_cnae_aprovacao_pipeline.run_extract_and_load(download_params)


br_bndes_estatisticas_operacoes_setor_cnae_aprovacao_download.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, SETOR_CNAE_APROVACAO_TABLE_ID
    )
)
_setor_cnae_aprovacao_pipeline.extract_load_deployment = (
    br_bndes_estatisticas_operacoes_setor_cnae_aprovacao_download.fn.__name__
)


_subsetor_cnae_aprovacao_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=SUBSETOR_CNAE_APROVACAO_TABLE_ID,
    get_latest_update=subsetor_cnae_aprovacao_get_latest_update,
    extract_load_data=subsetor_cnae_aprovacao_extract_load_data,
)


@flow(
    name=_subsetor_cnae_aprovacao_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_subsetor_cnae_aprovacao_check_update() -> (
    None
):
    _subsetor_cnae_aprovacao_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_subsetor_cnae_aprovacao_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, SUBSETOR_CNAE_APROVACAO_TABLE_ID
)
br_bndes_estatisticas_operacoes_subsetor_cnae_aprovacao_check_update.deploy_schedules = [
    Cron("30 0 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_subsetor_cnae_aprovacao_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_subsetor_cnae_aprovacao_download(
    download_params: dict,
) -> None:
    _subsetor_cnae_aprovacao_pipeline.run_extract_and_load(download_params)


br_bndes_estatisticas_operacoes_subsetor_cnae_aprovacao_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, SUBSETOR_CNAE_APROVACAO_TABLE_ID
)
_subsetor_cnae_aprovacao_pipeline.extract_load_deployment = br_bndes_estatisticas_operacoes_subsetor_cnae_aprovacao_download.fn.__name__


_setor_bndes_aprovacao_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=SETOR_BNDES_APROVACAO_TABLE_ID,
    get_latest_update=setor_bndes_aprovacao_get_latest_update,
    extract_load_data=setor_bndes_aprovacao_extract_load_data,
)


@flow(
    name=_setor_bndes_aprovacao_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_setor_bndes_aprovacao_check_update() -> (
    None
):
    _setor_bndes_aprovacao_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_setor_bndes_aprovacao_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, SETOR_BNDES_APROVACAO_TABLE_ID
)
br_bndes_estatisticas_operacoes_setor_bndes_aprovacao_check_update.deploy_schedules = [
    Cron("35 0 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_setor_bndes_aprovacao_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_setor_bndes_aprovacao_download(
    download_params: dict,
) -> None:
    _setor_bndes_aprovacao_pipeline.run_extract_and_load(download_params)


br_bndes_estatisticas_operacoes_setor_bndes_aprovacao_download.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, SETOR_BNDES_APROVACAO_TABLE_ID
    )
)
_setor_bndes_aprovacao_pipeline.extract_load_deployment = (
    br_bndes_estatisticas_operacoes_setor_bndes_aprovacao_download.fn.__name__
)


_subsetor_bndes_aprovacao_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=SUBSETOR_BNDES_APROVACAO_TABLE_ID,
    get_latest_update=subsetor_bndes_aprovacao_get_latest_update,
    extract_load_data=subsetor_bndes_aprovacao_extract_load_data,
)


@flow(
    name=_subsetor_bndes_aprovacao_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_subsetor_bndes_aprovacao_check_update() -> (
    None
):
    _subsetor_bndes_aprovacao_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_subsetor_bndes_aprovacao_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, SUBSETOR_BNDES_APROVACAO_TABLE_ID
)
br_bndes_estatisticas_operacoes_subsetor_bndes_aprovacao_check_update.deploy_schedules = [
    Cron("40 0 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_subsetor_bndes_aprovacao_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_subsetor_bndes_aprovacao_download(
    download_params: dict,
) -> None:
    _subsetor_bndes_aprovacao_pipeline.run_extract_and_load(download_params)


br_bndes_estatisticas_operacoes_subsetor_bndes_aprovacao_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, SUBSETOR_BNDES_APROVACAO_TABLE_ID
)
_subsetor_bndes_aprovacao_pipeline.extract_load_deployment = br_bndes_estatisticas_operacoes_subsetor_bndes_aprovacao_download.fn.__name__


_exim_aprovacao_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=EXIM_APROVACAO_TABLE_ID,
    get_latest_update=exim_aprovacao_get_latest_update,
    extract_load_data=exim_aprovacao_extract_load_data,
)


@flow(name=_exim_aprovacao_pipeline.check_update_flow_name, log_prints=True)
def br_bndes_estatisticas_operacoes_exim_aprovacao_check_update() -> None:
    _exim_aprovacao_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_exim_aprovacao_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, EXIM_APROVACAO_TABLE_ID)
)
br_bndes_estatisticas_operacoes_exim_aprovacao_check_update.deploy_schedules = [
    Cron("45 0 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_exim_aprovacao_pipeline.extract_and_load_flow_name, log_prints=True
)
def br_bndes_estatisticas_operacoes_exim_aprovacao_download(
    download_params: dict,
) -> None:
    _exim_aprovacao_pipeline.run_extract_and_load(download_params)


br_bndes_estatisticas_operacoes_exim_aprovacao_download.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.EXTRACT_AND_LOAD, EXIM_APROVACAO_TABLE_ID)
)
_exim_aprovacao_pipeline.extract_load_deployment = (
    br_bndes_estatisticas_operacoes_exim_aprovacao_download.fn.__name__
)


_produto_direta_aprovacao_dolar_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=PRODUTO_DIRETA_APROVACAO_DOLAR_TABLE_ID,
    get_latest_update=produto_direta_aprovacao_dolar_get_latest_update,
    extract_load_data=produto_direta_aprovacao_dolar_extract_load_data,
)


@flow(
    name=_produto_direta_aprovacao_dolar_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_produto_direta_aprovacao_dolar_check_update() -> (
    None
):
    _produto_direta_aprovacao_dolar_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_produto_direta_aprovacao_dolar_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, PRODUTO_DIRETA_APROVACAO_DOLAR_TABLE_ID
)
br_bndes_estatisticas_operacoes_produto_direta_aprovacao_dolar_check_update.deploy_schedules = [
    Cron("50 0 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_produto_direta_aprovacao_dolar_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_produto_direta_aprovacao_dolar_download(
    download_params: dict,
) -> None:
    _produto_direta_aprovacao_dolar_pipeline.run_extract_and_load(
        download_params
    )


br_bndes_estatisticas_operacoes_produto_direta_aprovacao_dolar_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, PRODUTO_DIRETA_APROVACAO_DOLAR_TABLE_ID
)
_produto_direta_aprovacao_dolar_pipeline.extract_load_deployment = br_bndes_estatisticas_operacoes_produto_direta_aprovacao_dolar_download.fn.__name__


_produto_indireta_aprovacao_dolar_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=PRODUTO_INDIRETA_APROVACAO_DOLAR_TABLE_ID,
    get_latest_update=produto_indireta_aprovacao_dolar_get_latest_update,
    extract_load_data=produto_indireta_aprovacao_dolar_extract_load_data,
)


@flow(
    name=_produto_indireta_aprovacao_dolar_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_produto_indireta_aprovacao_dolar_check_update() -> (
    None
):
    _produto_indireta_aprovacao_dolar_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_produto_indireta_aprovacao_dolar_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, PRODUTO_INDIRETA_APROVACAO_DOLAR_TABLE_ID
)
br_bndes_estatisticas_operacoes_produto_indireta_aprovacao_dolar_check_update.deploy_schedules = [
    Cron("55 0 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_produto_indireta_aprovacao_dolar_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_produto_indireta_aprovacao_dolar_download(
    download_params: dict,
) -> None:
    _produto_indireta_aprovacao_dolar_pipeline.run_extract_and_load(
        download_params
    )


br_bndes_estatisticas_operacoes_produto_indireta_aprovacao_dolar_download.deploy_tags = deploy_tags(
    DATASET_ID,
    Etapa.EXTRACT_AND_LOAD,
    PRODUTO_INDIRETA_APROVACAO_DOLAR_TABLE_ID,
)
_produto_indireta_aprovacao_dolar_pipeline.extract_load_deployment = br_bndes_estatisticas_operacoes_produto_indireta_aprovacao_dolar_download.fn.__name__


_exim_aprovacao_dolar_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=EXIM_APROVACAO_DOLAR_TABLE_ID,
    get_latest_update=exim_aprovacao_dolar_get_latest_update,
    extract_load_data=exim_aprovacao_dolar_extract_load_data,
)


@flow(
    name=_exim_aprovacao_dolar_pipeline.check_update_flow_name, log_prints=True
)
def br_bndes_estatisticas_operacoes_exim_aprovacao_dolar_check_update() -> (
    None
):
    _exim_aprovacao_dolar_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_exim_aprovacao_dolar_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, EXIM_APROVACAO_DOLAR_TABLE_ID
)
br_bndes_estatisticas_operacoes_exim_aprovacao_dolar_check_update.deploy_schedules = [
    Cron("5 1 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_exim_aprovacao_dolar_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_exim_aprovacao_dolar_download(
    download_params: dict,
) -> None:
    _exim_aprovacao_dolar_pipeline.run_extract_and_load(download_params)


br_bndes_estatisticas_operacoes_exim_aprovacao_dolar_download.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, EXIM_APROVACAO_DOLAR_TABLE_ID
    )
)
_exim_aprovacao_dolar_pipeline.extract_load_deployment = (
    br_bndes_estatisticas_operacoes_exim_aprovacao_dolar_download.fn.__name__
)


_exim_setor_cnae_aprovacao_dolar_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=EXIM_SETOR_CNAE_APROVACAO_DOLAR_TABLE_ID,
    get_latest_update=exim_setor_cnae_aprovacao_dolar_get_latest_update,
    extract_load_data=exim_setor_cnae_aprovacao_dolar_extract_load_data,
)


@flow(
    name=_exim_setor_cnae_aprovacao_dolar_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_exim_setor_cnae_aprovacao_dolar_check_update() -> (
    None
):
    _exim_setor_cnae_aprovacao_dolar_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_exim_setor_cnae_aprovacao_dolar_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, EXIM_SETOR_CNAE_APROVACAO_DOLAR_TABLE_ID
)
br_bndes_estatisticas_operacoes_exim_setor_cnae_aprovacao_dolar_check_update.deploy_schedules = [
    Cron("10 1 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_exim_setor_cnae_aprovacao_dolar_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_exim_setor_cnae_aprovacao_dolar_download(
    download_params: dict,
) -> None:
    _exim_setor_cnae_aprovacao_dolar_pipeline.run_extract_and_load(
        download_params
    )


br_bndes_estatisticas_operacoes_exim_setor_cnae_aprovacao_dolar_download.deploy_tags = deploy_tags(
    DATASET_ID,
    Etapa.EXTRACT_AND_LOAD,
    EXIM_SETOR_CNAE_APROVACAO_DOLAR_TABLE_ID,
)
_exim_setor_cnae_aprovacao_dolar_pipeline.extract_load_deployment = br_bndes_estatisticas_operacoes_exim_setor_cnae_aprovacao_dolar_download.fn.__name__


_exim_subsetor_cnae_aprovacao_dolar_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=EXIM_SUBSETOR_CNAE_APROVACAO_DOLAR_TABLE_ID,
    get_latest_update=exim_subsetor_cnae_aprovacao_dolar_get_latest_update,
    extract_load_data=exim_subsetor_cnae_aprovacao_dolar_extract_load_data,
)


@flow(
    name=_exim_subsetor_cnae_aprovacao_dolar_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_exim_subsetor_cnae_aprovacao_dolar_check_update() -> (
    None
):
    _exim_subsetor_cnae_aprovacao_dolar_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_exim_subsetor_cnae_aprovacao_dolar_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, EXIM_SUBSETOR_CNAE_APROVACAO_DOLAR_TABLE_ID
)
br_bndes_estatisticas_operacoes_exim_subsetor_cnae_aprovacao_dolar_check_update.deploy_schedules = [
    Cron("15 1 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_exim_subsetor_cnae_aprovacao_dolar_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_exim_subsetor_cnae_aprovacao_dolar_download(
    download_params: dict,
) -> None:
    _exim_subsetor_cnae_aprovacao_dolar_pipeline.run_extract_and_load(
        download_params
    )


br_bndes_estatisticas_operacoes_exim_subsetor_cnae_aprovacao_dolar_download.deploy_tags = deploy_tags(
    DATASET_ID,
    Etapa.EXTRACT_AND_LOAD,
    EXIM_SUBSETOR_CNAE_APROVACAO_DOLAR_TABLE_ID,
)
_exim_subsetor_cnae_aprovacao_dolar_pipeline.extract_load_deployment = br_bndes_estatisticas_operacoes_exim_subsetor_cnae_aprovacao_dolar_download.fn.__name__


_produto_direta_consulta_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=PRODUTO_DIRETA_CONSULTA_TABLE_ID,
    get_latest_update=produto_direta_consulta_get_latest_update,
    extract_load_data=produto_direta_consulta_extract_load_data,
)


@flow(
    name=_produto_direta_consulta_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_produto_direta_consulta_check_update() -> (
    None
):
    _produto_direta_consulta_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_produto_direta_consulta_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, PRODUTO_DIRETA_CONSULTA_TABLE_ID
)
br_bndes_estatisticas_operacoes_produto_direta_consulta_check_update.deploy_schedules = [
    Cron("20 1 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_produto_direta_consulta_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_produto_direta_consulta_download(
    download_params: dict,
) -> None:
    _produto_direta_consulta_pipeline.run_extract_and_load(download_params)


br_bndes_estatisticas_operacoes_produto_direta_consulta_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, PRODUTO_DIRETA_CONSULTA_TABLE_ID
)
_produto_direta_consulta_pipeline.extract_load_deployment = br_bndes_estatisticas_operacoes_produto_direta_consulta_download.fn.__name__


_produto_indireta_consulta_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=PRODUTO_INDIRETA_CONSULTA_TABLE_ID,
    get_latest_update=produto_indireta_consulta_get_latest_update,
    extract_load_data=produto_indireta_consulta_extract_load_data,
)


@flow(
    name=_produto_indireta_consulta_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_produto_indireta_consulta_check_update() -> (
    None
):
    _produto_indireta_consulta_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_produto_indireta_consulta_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, PRODUTO_INDIRETA_CONSULTA_TABLE_ID
)
br_bndes_estatisticas_operacoes_produto_indireta_consulta_check_update.deploy_schedules = [
    Cron("25 1 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_produto_indireta_consulta_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_produto_indireta_consulta_download(
    download_params: dict,
) -> None:
    _produto_indireta_consulta_pipeline.run_extract_and_load(download_params)


br_bndes_estatisticas_operacoes_produto_indireta_consulta_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, PRODUTO_INDIRETA_CONSULTA_TABLE_ID
)
_produto_indireta_consulta_pipeline.extract_load_deployment = br_bndes_estatisticas_operacoes_produto_indireta_consulta_download.fn.__name__


_porte_consulta_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=PORTE_CONSULTA_TABLE_ID,
    get_latest_update=porte_consulta_get_latest_update,
    extract_load_data=porte_consulta_extract_load_data,
)


@flow(name=_porte_consulta_pipeline.check_update_flow_name, log_prints=True)
def br_bndes_estatisticas_operacoes_porte_consulta_check_update() -> None:
    _porte_consulta_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_porte_consulta_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, PORTE_CONSULTA_TABLE_ID)
)
br_bndes_estatisticas_operacoes_porte_consulta_check_update.deploy_schedules = [
    Cron("30 1 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_porte_consulta_pipeline.extract_and_load_flow_name, log_prints=True
)
def br_bndes_estatisticas_operacoes_porte_consulta_download(
    download_params: dict,
) -> None:
    _porte_consulta_pipeline.run_extract_and_load(download_params)


br_bndes_estatisticas_operacoes_porte_consulta_download.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.EXTRACT_AND_LOAD, PORTE_CONSULTA_TABLE_ID)
)
_porte_consulta_pipeline.extract_load_deployment = (
    br_bndes_estatisticas_operacoes_porte_consulta_download.fn.__name__
)


_uf_consulta_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=UF_CONSULTA_TABLE_ID,
    get_latest_update=uf_consulta_get_latest_update,
    extract_load_data=uf_consulta_extract_load_data,
)


@flow(name=_uf_consulta_pipeline.check_update_flow_name, log_prints=True)
def br_bndes_estatisticas_operacoes_uf_consulta_check_update() -> None:
    _uf_consulta_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_uf_consulta_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, UF_CONSULTA_TABLE_ID)
)
br_bndes_estatisticas_operacoes_uf_consulta_check_update.deploy_schedules = [
    Cron("35 1 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_uf_consulta_pipeline.extract_and_load_flow_name, log_prints=True)
def br_bndes_estatisticas_operacoes_uf_consulta_download(
    download_params: dict,
) -> None:
    _uf_consulta_pipeline.run_extract_and_load(download_params)


br_bndes_estatisticas_operacoes_uf_consulta_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, UF_CONSULTA_TABLE_ID
)
_uf_consulta_pipeline.extract_load_deployment = (
    br_bndes_estatisticas_operacoes_uf_consulta_download.fn.__name__
)


_setor_cnae_consulta_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=SETOR_CNAE_CONSULTA_TABLE_ID,
    get_latest_update=setor_cnae_consulta_get_latest_update,
    extract_load_data=setor_cnae_consulta_extract_load_data,
)


@flow(
    name=_setor_cnae_consulta_pipeline.check_update_flow_name, log_prints=True
)
def br_bndes_estatisticas_operacoes_setor_cnae_consulta_check_update() -> None:
    _setor_cnae_consulta_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_setor_cnae_consulta_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, SETOR_CNAE_CONSULTA_TABLE_ID
)
br_bndes_estatisticas_operacoes_setor_cnae_consulta_check_update.deploy_schedules = [
    Cron("40 1 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_setor_cnae_consulta_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_setor_cnae_consulta_download(
    download_params: dict,
) -> None:
    _setor_cnae_consulta_pipeline.run_extract_and_load(download_params)


br_bndes_estatisticas_operacoes_setor_cnae_consulta_download.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, SETOR_CNAE_CONSULTA_TABLE_ID
    )
)
_setor_cnae_consulta_pipeline.extract_load_deployment = (
    br_bndes_estatisticas_operacoes_setor_cnae_consulta_download.fn.__name__
)


_subsetor_cnae_consulta_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=SUBSETOR_CNAE_CONSULTA_TABLE_ID,
    get_latest_update=subsetor_cnae_consulta_get_latest_update,
    extract_load_data=subsetor_cnae_consulta_extract_load_data,
)


@flow(
    name=_subsetor_cnae_consulta_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_subsetor_cnae_consulta_check_update() -> (
    None
):
    _subsetor_cnae_consulta_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_subsetor_cnae_consulta_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, SUBSETOR_CNAE_CONSULTA_TABLE_ID
)
br_bndes_estatisticas_operacoes_subsetor_cnae_consulta_check_update.deploy_schedules = [
    Cron("50 1 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_subsetor_cnae_consulta_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_subsetor_cnae_consulta_download(
    download_params: dict,
) -> None:
    _subsetor_cnae_consulta_pipeline.run_extract_and_load(download_params)


br_bndes_estatisticas_operacoes_subsetor_cnae_consulta_download.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, SUBSETOR_CNAE_CONSULTA_TABLE_ID
    )
)
_subsetor_cnae_consulta_pipeline.extract_load_deployment = (
    br_bndes_estatisticas_operacoes_subsetor_cnae_consulta_download.fn.__name__
)


_setor_bndes_consulta_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=SETOR_BNDES_CONSULTA_TABLE_ID,
    get_latest_update=setor_bndes_consulta_get_latest_update,
    extract_load_data=setor_bndes_consulta_extract_load_data,
)


@flow(
    name=_setor_bndes_consulta_pipeline.check_update_flow_name, log_prints=True
)
def br_bndes_estatisticas_operacoes_setor_bndes_consulta_check_update() -> (
    None
):
    _setor_bndes_consulta_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_setor_bndes_consulta_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, SETOR_BNDES_CONSULTA_TABLE_ID
)
br_bndes_estatisticas_operacoes_setor_bndes_consulta_check_update.deploy_schedules = [
    Cron("55 1 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_setor_bndes_consulta_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_setor_bndes_consulta_download(
    download_params: dict,
) -> None:
    _setor_bndes_consulta_pipeline.run_extract_and_load(download_params)


br_bndes_estatisticas_operacoes_setor_bndes_consulta_download.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, SETOR_BNDES_CONSULTA_TABLE_ID
    )
)
_setor_bndes_consulta_pipeline.extract_load_deployment = (
    br_bndes_estatisticas_operacoes_setor_bndes_consulta_download.fn.__name__
)


_subsetor_bndes_consulta_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=SUBSETOR_BNDES_CONSULTA_TABLE_ID,
    get_latest_update=subsetor_bndes_consulta_get_latest_update,
    extract_load_data=subsetor_bndes_consulta_extract_load_data,
)


@flow(
    name=_subsetor_bndes_consulta_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_subsetor_bndes_consulta_check_update() -> (
    None
):
    _subsetor_bndes_consulta_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_subsetor_bndes_consulta_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, SUBSETOR_BNDES_CONSULTA_TABLE_ID
)
br_bndes_estatisticas_operacoes_subsetor_bndes_consulta_check_update.deploy_schedules = [
    Cron("5 2 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_subsetor_bndes_consulta_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_subsetor_bndes_consulta_download(
    download_params: dict,
) -> None:
    _subsetor_bndes_consulta_pipeline.run_extract_and_load(download_params)


br_bndes_estatisticas_operacoes_subsetor_bndes_consulta_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, SUBSETOR_BNDES_CONSULTA_TABLE_ID
)
_subsetor_bndes_consulta_pipeline.extract_load_deployment = br_bndes_estatisticas_operacoes_subsetor_bndes_consulta_download.fn.__name__


_exim_consulta_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=EXIM_CONSULTA_TABLE_ID,
    get_latest_update=exim_consulta_get_latest_update,
    extract_load_data=exim_consulta_extract_load_data,
)


@flow(name=_exim_consulta_pipeline.check_update_flow_name, log_prints=True)
def br_bndes_estatisticas_operacoes_exim_consulta_check_update() -> None:
    _exim_consulta_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_exim_consulta_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, EXIM_CONSULTA_TABLE_ID)
)
br_bndes_estatisticas_operacoes_exim_consulta_check_update.deploy_schedules = [
    Cron("15 2 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_exim_consulta_pipeline.extract_and_load_flow_name, log_prints=True)
def br_bndes_estatisticas_operacoes_exim_consulta_download(
    download_params: dict,
) -> None:
    _exim_consulta_pipeline.run_extract_and_load(download_params)


br_bndes_estatisticas_operacoes_exim_consulta_download.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.EXTRACT_AND_LOAD, EXIM_CONSULTA_TABLE_ID)
)
_exim_consulta_pipeline.extract_load_deployment = (
    br_bndes_estatisticas_operacoes_exim_consulta_download.fn.__name__
)


_produto_direta_consulta_dolar_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=PRODUTO_DIRETA_CONSULTA_DOLAR_TABLE_ID,
    get_latest_update=produto_direta_consulta_dolar_get_latest_update,
    extract_load_data=produto_direta_consulta_dolar_extract_load_data,
)


@flow(
    name=_produto_direta_consulta_dolar_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_produto_direta_consulta_dolar_check_update() -> (
    None
):
    _produto_direta_consulta_dolar_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_produto_direta_consulta_dolar_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, PRODUTO_DIRETA_CONSULTA_DOLAR_TABLE_ID
)
br_bndes_estatisticas_operacoes_produto_direta_consulta_dolar_check_update.deploy_schedules = [
    Cron("20 2 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_produto_direta_consulta_dolar_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_produto_direta_consulta_dolar_download(
    download_params: dict,
) -> None:
    _produto_direta_consulta_dolar_pipeline.run_extract_and_load(
        download_params
    )


br_bndes_estatisticas_operacoes_produto_direta_consulta_dolar_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, PRODUTO_DIRETA_CONSULTA_DOLAR_TABLE_ID
)
_produto_direta_consulta_dolar_pipeline.extract_load_deployment = br_bndes_estatisticas_operacoes_produto_direta_consulta_dolar_download.fn.__name__


_produto_indireta_consulta_dolar_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=PRODUTO_INDIRETA_CONSULTA_DOLAR_TABLE_ID,
    get_latest_update=produto_indireta_consulta_dolar_get_latest_update,
    extract_load_data=produto_indireta_consulta_dolar_extract_load_data,
)


@flow(
    name=_produto_indireta_consulta_dolar_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_produto_indireta_consulta_dolar_check_update() -> (
    None
):
    _produto_indireta_consulta_dolar_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_produto_indireta_consulta_dolar_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, PRODUTO_INDIRETA_CONSULTA_DOLAR_TABLE_ID
)
br_bndes_estatisticas_operacoes_produto_indireta_consulta_dolar_check_update.deploy_schedules = [
    Cron("30 2 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_produto_indireta_consulta_dolar_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_produto_indireta_consulta_dolar_download(
    download_params: dict,
) -> None:
    _produto_indireta_consulta_dolar_pipeline.run_extract_and_load(
        download_params
    )


br_bndes_estatisticas_operacoes_produto_indireta_consulta_dolar_download.deploy_tags = deploy_tags(
    DATASET_ID,
    Etapa.EXTRACT_AND_LOAD,
    PRODUTO_INDIRETA_CONSULTA_DOLAR_TABLE_ID,
)
_produto_indireta_consulta_dolar_pipeline.extract_load_deployment = br_bndes_estatisticas_operacoes_produto_indireta_consulta_dolar_download.fn.__name__


_exim_consulta_dolar_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=EXIM_CONSULTA_DOLAR_TABLE_ID,
    get_latest_update=exim_consulta_dolar_get_latest_update,
    extract_load_data=exim_consulta_dolar_extract_load_data,
)


@flow(
    name=_exim_consulta_dolar_pipeline.check_update_flow_name, log_prints=True
)
def br_bndes_estatisticas_operacoes_exim_consulta_dolar_check_update() -> None:
    _exim_consulta_dolar_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_exim_consulta_dolar_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, EXIM_CONSULTA_DOLAR_TABLE_ID
)
br_bndes_estatisticas_operacoes_exim_consulta_dolar_check_update.deploy_schedules = [
    Cron("45 2 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_exim_consulta_dolar_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_exim_consulta_dolar_download(
    download_params: dict,
) -> None:
    _exim_consulta_dolar_pipeline.run_extract_and_load(download_params)


br_bndes_estatisticas_operacoes_exim_consulta_dolar_download.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, EXIM_CONSULTA_DOLAR_TABLE_ID
    )
)
_exim_consulta_dolar_pipeline.extract_load_deployment = (
    br_bndes_estatisticas_operacoes_exim_consulta_dolar_download.fn.__name__
)


_exim_setor_cnae_consulta_dolar_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=EXIM_SETOR_CNAE_CONSULTA_DOLAR_TABLE_ID,
    get_latest_update=exim_setor_cnae_consulta_dolar_get_latest_update,
    extract_load_data=exim_setor_cnae_consulta_dolar_extract_load_data,
)


@flow(
    name=_exim_setor_cnae_consulta_dolar_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_exim_setor_cnae_consulta_dolar_check_update() -> (
    None
):
    _exim_setor_cnae_consulta_dolar_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_exim_setor_cnae_consulta_dolar_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, EXIM_SETOR_CNAE_CONSULTA_DOLAR_TABLE_ID
)
br_bndes_estatisticas_operacoes_exim_setor_cnae_consulta_dolar_check_update.deploy_schedules = [
    Cron("55 2 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_exim_setor_cnae_consulta_dolar_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_exim_setor_cnae_consulta_dolar_download(
    download_params: dict,
) -> None:
    _exim_setor_cnae_consulta_dolar_pipeline.run_extract_and_load(
        download_params
    )


br_bndes_estatisticas_operacoes_exim_setor_cnae_consulta_dolar_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, EXIM_SETOR_CNAE_CONSULTA_DOLAR_TABLE_ID
)
_exim_setor_cnae_consulta_dolar_pipeline.extract_load_deployment = br_bndes_estatisticas_operacoes_exim_setor_cnae_consulta_dolar_download.fn.__name__


_exim_subsetor_cnae_consulta_dolar_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=EXIM_SUBSETOR_CNAE_CONSULTA_DOLAR_TABLE_ID,
    get_latest_update=exim_subsetor_cnae_consulta_dolar_get_latest_update,
    extract_load_data=exim_subsetor_cnae_consulta_dolar_extract_load_data,
)


@flow(
    name=_exim_subsetor_cnae_consulta_dolar_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_exim_subsetor_cnae_consulta_dolar_check_update() -> (
    None
):
    _exim_subsetor_cnae_consulta_dolar_pipeline.run_check_update()


br_bndes_estatisticas_operacoes_exim_subsetor_cnae_consulta_dolar_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, EXIM_SUBSETOR_CNAE_CONSULTA_DOLAR_TABLE_ID
)
br_bndes_estatisticas_operacoes_exim_subsetor_cnae_consulta_dolar_check_update.deploy_schedules = [
    Cron("5 3 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_exim_subsetor_cnae_consulta_dolar_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_bndes_estatisticas_operacoes_exim_subsetor_cnae_consulta_dolar_download(
    download_params: dict,
) -> None:
    _exim_subsetor_cnae_consulta_dolar_pipeline.run_extract_and_load(
        download_params
    )


br_bndes_estatisticas_operacoes_exim_subsetor_cnae_consulta_dolar_download.deploy_tags = deploy_tags(
    DATASET_ID,
    Etapa.EXTRACT_AND_LOAD,
    EXIM_SUBSETOR_CNAE_CONSULTA_DOLAR_TABLE_ID,
)
_exim_subsetor_cnae_consulta_dolar_pipeline.extract_load_deployment = br_bndes_estatisticas_operacoes_exim_subsetor_cnae_consulta_dolar_download.fn.__name__
