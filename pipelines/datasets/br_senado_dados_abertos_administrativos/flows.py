"""Flows de br_senado_dados_abertos_administrativos — Prefect 3, modelo em estágios."""

from prefect.schedules import Cron

from pipelines.datasets.br_senado_dados_abertos_administrativos.constants import (
    constants,
    constants_ata_acionamento,
    constants_contratacao,
    constants_contratacao_documento_fiscal,
    constants_contratacao_garantia,
    constants_contratacao_item,
    constants_contratacao_orgao_gestor,
    constants_contratacao_pagamento,
    constants_contratacao_pagamento_empenho,
    constants_contrato_aditivo,
    constants_despesa_ceaps,
    constants_diretor_coordenador,
    constants_empresa,
    constants_estagiario,
    constants_licitacao,
    constants_licitacao_detalhamento,
    constants_menor_aprendiz,
    constants_pensionista,
    constants_previsao_aposentadoria,
    constants_quadro_pessoal,
    constants_senador,
    constants_senador_aposentado_pensionista,
    constants_senador_auxilio_moradia,
    constants_senador_escritorio_apoio,
    constants_senador_gabinete,
    constants_servidor,
    constants_servidor_aposentado,
    constants_servidor_ativo,
    constants_servidor_cedido,
    constants_servidor_exonerado,
    constants_servidor_hora_extra,
    constants_servidor_hora_extra_dia,
    constants_servidor_remuneracao,
    constants_suprido_ato_concessao,
    constants_suprido_empenho,
    constants_suprido_movimentacao,
    constants_suprido_movimentacao_subtipo,
    constants_suprido_transacao,
    constants_suprido_transacao_objeto,
    constants_terceirizado,
)
from pipelines.datasets.br_senado_dados_abertos_administrativos.tasks import (
    make_pipeline,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

_senador_pipeline = make_pipeline(constants_senador.TABLE_ID.value)


@flow(name=_senador_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_senador_check_update() -> None:
    _senador_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_senador_check_update.deploy_tags = (
    deploy_tags(
        constants.DATASET_ID.value,
        Etapa.CHECK_UPDATE,
        constants_senador.TABLE_ID.value,
    )
)
br_senado_dados_abertos_administrativos_senador_check_update.deploy_schedules = [
    Cron("27 10 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_senador_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_senador_download(
    download_params: dict,
) -> None:
    _senador_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_senador_download.deploy_tags = (
    deploy_tags(
        constants.DATASET_ID.value,
        Etapa.EXTRACT_AND_LOAD,
        constants_senador.TABLE_ID.value,
    )
)
_senador_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_administrativos_senador_download.fn.__name__
)


_senador_gabinete_pipeline = make_pipeline(
    constants_senador_gabinete.TABLE_ID.value
)


@flow(name=_senador_gabinete_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_senador_gabinete_check_update() -> (
    None
):
    _senador_gabinete_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_senador_gabinete_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_senador_gabinete.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_senador_gabinete_check_update.deploy_schedules = [
    Cron("48 10 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_senador_gabinete_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_senador_gabinete_download(
    download_params: dict,
) -> None:
    _senador_gabinete_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_senador_gabinete_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_senador_gabinete.TABLE_ID.value,
)
_senador_gabinete_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_senador_gabinete_download.fn.__name__


_senador_escritorio_apoio_pipeline = make_pipeline(
    constants_senador_escritorio_apoio.TABLE_ID.value
)


@flow(
    name=_senador_escritorio_apoio_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_senador_escritorio_apoio_check_update() -> (
    None
):
    _senador_escritorio_apoio_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_senador_escritorio_apoio_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_senador_escritorio_apoio.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_senador_escritorio_apoio_check_update.deploy_schedules = [
    Cron("53 10 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_senador_escritorio_apoio_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_senador_escritorio_apoio_download(
    download_params: dict,
) -> None:
    _senador_escritorio_apoio_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_senador_escritorio_apoio_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_senador_escritorio_apoio.TABLE_ID.value,
)
_senador_escritorio_apoio_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_senador_escritorio_apoio_download.fn.__name__


_senador_auxilio_moradia_pipeline = make_pipeline(
    constants_senador_auxilio_moradia.TABLE_ID.value
)


@flow(
    name=_senador_auxilio_moradia_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_senador_auxilio_moradia_check_update() -> (
    None
):
    _senador_auxilio_moradia_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_senador_auxilio_moradia_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_senador_auxilio_moradia.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_senador_auxilio_moradia_check_update.deploy_schedules = [
    Cron("58 10 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_senador_auxilio_moradia_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_senador_auxilio_moradia_download(
    download_params: dict,
) -> None:
    _senador_auxilio_moradia_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_senador_auxilio_moradia_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_senador_auxilio_moradia.TABLE_ID.value,
)
_senador_auxilio_moradia_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_senador_auxilio_moradia_download.fn.__name__


_senador_aposentado_pensionista_pipeline = make_pipeline(
    constants_senador_aposentado_pensionista.TABLE_ID.value
)


@flow(
    name=_senador_aposentado_pensionista_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_senador_aposentado_pensionista_check_update() -> (
    None
):
    _senador_aposentado_pensionista_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_senador_aposentado_pensionista_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_senador_aposentado_pensionista.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_senador_aposentado_pensionista_check_update.deploy_schedules = [
    Cron("3 11 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_senador_aposentado_pensionista_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_senador_aposentado_pensionista_download(
    download_params: dict,
) -> None:
    _senador_aposentado_pensionista_pipeline.run_extract_and_load(
        download_params
    )


br_senado_dados_abertos_administrativos_senador_aposentado_pensionista_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_senador_aposentado_pensionista.TABLE_ID.value,
)
_senador_aposentado_pensionista_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_senador_aposentado_pensionista_download.fn.__name__


_despesa_ceaps_pipeline = make_pipeline(constants_despesa_ceaps.TABLE_ID.value)


@flow(name=_despesa_ceaps_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_despesa_ceaps_check_update() -> (
    None
):
    _despesa_ceaps_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_despesa_ceaps_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_despesa_ceaps.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_despesa_ceaps_check_update.deploy_schedules = [
    Cron("8 11 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_despesa_ceaps_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_despesa_ceaps_download(
    download_params: dict,
) -> None:
    _despesa_ceaps_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_despesa_ceaps_download.deploy_tags = (
    deploy_tags(
        constants.DATASET_ID.value,
        Etapa.EXTRACT_AND_LOAD,
        constants_despesa_ceaps.TABLE_ID.value,
    )
)
_despesa_ceaps_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_administrativos_despesa_ceaps_download.fn.__name__
)


_servidor_pipeline = make_pipeline(constants_servidor.TABLE_ID.value)


@flow(name=_servidor_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_servidor_check_update() -> None:
    _servidor_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_servidor_check_update.deploy_tags = (
    deploy_tags(
        constants.DATASET_ID.value,
        Etapa.CHECK_UPDATE,
        constants_servidor.TABLE_ID.value,
    )
)
br_senado_dados_abertos_administrativos_servidor_check_update.deploy_schedules = [
    Cron("13 11 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_servidor_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_servidor_download(
    download_params: dict,
) -> None:
    _servidor_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_servidor_download.deploy_tags = (
    deploy_tags(
        constants.DATASET_ID.value,
        Etapa.EXTRACT_AND_LOAD,
        constants_servidor.TABLE_ID.value,
    )
)
_servidor_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_administrativos_servidor_download.fn.__name__
)


_servidor_ativo_pipeline = make_pipeline(
    constants_servidor_ativo.TABLE_ID.value
)


@flow(name=_servidor_ativo_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_servidor_ativo_check_update() -> (
    None
):
    _servidor_ativo_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_servidor_ativo_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_servidor_ativo.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_servidor_ativo_check_update.deploy_schedules = [
    Cron("18 11 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_servidor_ativo_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_servidor_ativo_download(
    download_params: dict,
) -> None:
    _servidor_ativo_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_servidor_ativo_download.deploy_tags = (
    deploy_tags(
        constants.DATASET_ID.value,
        Etapa.EXTRACT_AND_LOAD,
        constants_servidor_ativo.TABLE_ID.value,
    )
)
_servidor_ativo_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_administrativos_servidor_ativo_download.fn.__name__
)


_servidor_aposentado_pipeline = make_pipeline(
    constants_servidor_aposentado.TABLE_ID.value
)


@flow(
    name=_servidor_aposentado_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_servidor_aposentado_check_update() -> (
    None
):
    _servidor_aposentado_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_servidor_aposentado_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_servidor_aposentado.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_servidor_aposentado_check_update.deploy_schedules = [
    Cron("23 11 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_servidor_aposentado_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_servidor_aposentado_download(
    download_params: dict,
) -> None:
    _servidor_aposentado_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_servidor_aposentado_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_servidor_aposentado.TABLE_ID.value,
)
_servidor_aposentado_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_servidor_aposentado_download.fn.__name__


_servidor_exonerado_pipeline = make_pipeline(
    constants_servidor_exonerado.TABLE_ID.value
)


@flow(
    name=_servidor_exonerado_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_servidor_exonerado_check_update() -> (
    None
):
    _servidor_exonerado_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_servidor_exonerado_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_servidor_exonerado.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_servidor_exonerado_check_update.deploy_schedules = [
    Cron("28 11 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_servidor_exonerado_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_servidor_exonerado_download(
    download_params: dict,
) -> None:
    _servidor_exonerado_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_servidor_exonerado_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_servidor_exonerado.TABLE_ID.value,
)
_servidor_exonerado_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_servidor_exonerado_download.fn.__name__


_servidor_cedido_pipeline = make_pipeline(
    constants_servidor_cedido.TABLE_ID.value
)


@flow(name=_servidor_cedido_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_servidor_cedido_check_update() -> (
    None
):
    _servidor_cedido_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_servidor_cedido_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_servidor_cedido.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_servidor_cedido_check_update.deploy_schedules = [
    Cron("33 11 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_servidor_cedido_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_servidor_cedido_download(
    download_params: dict,
) -> None:
    _servidor_cedido_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_servidor_cedido_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_servidor_cedido.TABLE_ID.value,
)
_servidor_cedido_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_servidor_cedido_download.fn.__name__


_pensionista_pipeline = make_pipeline(constants_pensionista.TABLE_ID.value)


@flow(name=_pensionista_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_pensionista_check_update() -> None:
    _pensionista_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_pensionista_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_pensionista.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_pensionista_check_update.deploy_schedules = [
    Cron("43 11 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_pensionista_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_pensionista_download(
    download_params: dict,
) -> None:
    _pensionista_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_pensionista_download.deploy_tags = (
    deploy_tags(
        constants.DATASET_ID.value,
        Etapa.EXTRACT_AND_LOAD,
        constants_pensionista.TABLE_ID.value,
    )
)
_pensionista_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_administrativos_pensionista_download.fn.__name__
)


_servidor_remuneracao_pipeline = make_pipeline(
    constants_servidor_remuneracao.TABLE_ID.value
)


@flow(
    name=_servidor_remuneracao_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_servidor_remuneracao_check_update() -> (
    None
):
    _servidor_remuneracao_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_servidor_remuneracao_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_servidor_remuneracao.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_servidor_remuneracao_check_update.deploy_schedules = [
    Cron("48 11 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_servidor_remuneracao_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_servidor_remuneracao_download(
    download_params: dict,
) -> None:
    _servidor_remuneracao_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_servidor_remuneracao_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_servidor_remuneracao.TABLE_ID.value,
)
_servidor_remuneracao_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_servidor_remuneracao_download.fn.__name__


_servidor_hora_extra_pipeline = make_pipeline(
    constants_servidor_hora_extra.TABLE_ID.value
)


@flow(
    name=_servidor_hora_extra_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_servidor_hora_extra_check_update() -> (
    None
):
    _servidor_hora_extra_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_servidor_hora_extra_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_servidor_hora_extra.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_servidor_hora_extra_check_update.deploy_schedules = [
    Cron("53 11 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_servidor_hora_extra_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_servidor_hora_extra_download(
    download_params: dict,
) -> None:
    _servidor_hora_extra_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_servidor_hora_extra_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_servidor_hora_extra.TABLE_ID.value,
)
_servidor_hora_extra_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_servidor_hora_extra_download.fn.__name__


_servidor_hora_extra_dia_pipeline = make_pipeline(
    constants_servidor_hora_extra_dia.TABLE_ID.value
)


@flow(
    name=_servidor_hora_extra_dia_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_servidor_hora_extra_dia_check_update() -> (
    None
):
    _servidor_hora_extra_dia_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_servidor_hora_extra_dia_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_servidor_hora_extra_dia.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_servidor_hora_extra_dia_check_update.deploy_schedules = [
    Cron("58 11 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_servidor_hora_extra_dia_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_servidor_hora_extra_dia_download(
    download_params: dict,
) -> None:
    _servidor_hora_extra_dia_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_servidor_hora_extra_dia_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_servidor_hora_extra_dia.TABLE_ID.value,
)
_servidor_hora_extra_dia_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_servidor_hora_extra_dia_download.fn.__name__


_contratacao_pipeline = make_pipeline(constants_contratacao.TABLE_ID.value)


@flow(name=_contratacao_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_contratacao_check_update() -> None:
    _contratacao_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_contratacao_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_contratacao.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_contratacao_check_update.deploy_schedules = [
    Cron("3 12 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_contratacao_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_contratacao_download(
    download_params: dict,
) -> None:
    _contratacao_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_contratacao_download.deploy_tags = (
    deploy_tags(
        constants.DATASET_ID.value,
        Etapa.EXTRACT_AND_LOAD,
        constants_contratacao.TABLE_ID.value,
    )
)
_contratacao_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_administrativos_contratacao_download.fn.__name__
)


_contratacao_orgao_gestor_pipeline = make_pipeline(
    constants_contratacao_orgao_gestor.TABLE_ID.value
)


@flow(
    name=_contratacao_orgao_gestor_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_contratacao_orgao_gestor_check_update() -> (
    None
):
    _contratacao_orgao_gestor_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_contratacao_orgao_gestor_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_contratacao_orgao_gestor.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_contratacao_orgao_gestor_check_update.deploy_schedules = [
    Cron("8 12 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_contratacao_orgao_gestor_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_contratacao_orgao_gestor_download(
    download_params: dict,
) -> None:
    _contratacao_orgao_gestor_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_contratacao_orgao_gestor_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_contratacao_orgao_gestor.TABLE_ID.value,
)
_contratacao_orgao_gestor_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_contratacao_orgao_gestor_download.fn.__name__


_licitacao_pipeline = make_pipeline(constants_licitacao.TABLE_ID.value)


@flow(name=_licitacao_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_licitacao_check_update() -> None:
    _licitacao_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_licitacao_check_update.deploy_tags = (
    deploy_tags(
        constants.DATASET_ID.value,
        Etapa.CHECK_UPDATE,
        constants_licitacao.TABLE_ID.value,
    )
)
br_senado_dados_abertos_administrativos_licitacao_check_update.deploy_schedules = [
    Cron("13 12 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_licitacao_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_licitacao_download(
    download_params: dict,
) -> None:
    _licitacao_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_licitacao_download.deploy_tags = (
    deploy_tags(
        constants.DATASET_ID.value,
        Etapa.EXTRACT_AND_LOAD,
        constants_licitacao.TABLE_ID.value,
    )
)
_licitacao_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_administrativos_licitacao_download.fn.__name__
)


_licitacao_detalhamento_pipeline = make_pipeline(
    constants_licitacao_detalhamento.TABLE_ID.value
)


@flow(
    name=_licitacao_detalhamento_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_licitacao_detalhamento_check_update() -> (
    None
):
    _licitacao_detalhamento_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_licitacao_detalhamento_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_licitacao_detalhamento.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_licitacao_detalhamento_check_update.deploy_schedules = [
    Cron("18 12 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_licitacao_detalhamento_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_licitacao_detalhamento_download(
    download_params: dict,
) -> None:
    _licitacao_detalhamento_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_licitacao_detalhamento_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_licitacao_detalhamento.TABLE_ID.value,
)
_licitacao_detalhamento_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_licitacao_detalhamento_download.fn.__name__


_empresa_pipeline = make_pipeline(constants_empresa.TABLE_ID.value)


@flow(name=_empresa_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_empresa_check_update() -> None:
    _empresa_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_empresa_check_update.deploy_tags = (
    deploy_tags(
        constants.DATASET_ID.value,
        Etapa.CHECK_UPDATE,
        constants_empresa.TABLE_ID.value,
    )
)
br_senado_dados_abertos_administrativos_empresa_check_update.deploy_schedules = [
    Cron("23 12 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_empresa_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_empresa_download(
    download_params: dict,
) -> None:
    _empresa_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_empresa_download.deploy_tags = (
    deploy_tags(
        constants.DATASET_ID.value,
        Etapa.EXTRACT_AND_LOAD,
        constants_empresa.TABLE_ID.value,
    )
)
_empresa_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_administrativos_empresa_download.fn.__name__
)


_terceirizado_pipeline = make_pipeline(constants_terceirizado.TABLE_ID.value)


@flow(name=_terceirizado_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_terceirizado_check_update() -> (
    None
):
    _terceirizado_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_terceirizado_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_terceirizado.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_terceirizado_check_update.deploy_schedules = [
    Cron("28 12 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_terceirizado_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_terceirizado_download(
    download_params: dict,
) -> None:
    _terceirizado_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_terceirizado_download.deploy_tags = (
    deploy_tags(
        constants.DATASET_ID.value,
        Etapa.EXTRACT_AND_LOAD,
        constants_terceirizado.TABLE_ID.value,
    )
)
_terceirizado_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_administrativos_terceirizado_download.fn.__name__
)


_menor_aprendiz_pipeline = make_pipeline(
    constants_menor_aprendiz.TABLE_ID.value
)


@flow(name=_menor_aprendiz_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_menor_aprendiz_check_update() -> (
    None
):
    _menor_aprendiz_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_menor_aprendiz_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_menor_aprendiz.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_menor_aprendiz_check_update.deploy_schedules = [
    Cron("33 12 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_menor_aprendiz_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_menor_aprendiz_download(
    download_params: dict,
) -> None:
    _menor_aprendiz_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_menor_aprendiz_download.deploy_tags = (
    deploy_tags(
        constants.DATASET_ID.value,
        Etapa.EXTRACT_AND_LOAD,
        constants_menor_aprendiz.TABLE_ID.value,
    )
)
_menor_aprendiz_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_administrativos_menor_aprendiz_download.fn.__name__
)


_estagiario_pipeline = make_pipeline(constants_estagiario.TABLE_ID.value)


@flow(name=_estagiario_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_estagiario_check_update() -> None:
    _estagiario_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_estagiario_check_update.deploy_tags = (
    deploy_tags(
        constants.DATASET_ID.value,
        Etapa.CHECK_UPDATE,
        constants_estagiario.TABLE_ID.value,
    )
)
br_senado_dados_abertos_administrativos_estagiario_check_update.deploy_schedules = [
    Cron("38 12 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_estagiario_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_estagiario_download(
    download_params: dict,
) -> None:
    _estagiario_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_estagiario_download.deploy_tags = (
    deploy_tags(
        constants.DATASET_ID.value,
        Etapa.EXTRACT_AND_LOAD,
        constants_estagiario.TABLE_ID.value,
    )
)
_estagiario_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_administrativos_estagiario_download.fn.__name__
)


_suprido_ato_concessao_pipeline = make_pipeline(
    constants_suprido_ato_concessao.TABLE_ID.value
)


@flow(
    name=_suprido_ato_concessao_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_suprido_ato_concessao_check_update() -> (
    None
):
    _suprido_ato_concessao_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_suprido_ato_concessao_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_suprido_ato_concessao.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_suprido_ato_concessao_check_update.deploy_schedules = [
    Cron("43 12 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_suprido_ato_concessao_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_suprido_ato_concessao_download(
    download_params: dict,
) -> None:
    _suprido_ato_concessao_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_suprido_ato_concessao_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_suprido_ato_concessao.TABLE_ID.value,
)
_suprido_ato_concessao_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_suprido_ato_concessao_download.fn.__name__


_suprido_empenho_pipeline = make_pipeline(
    constants_suprido_empenho.TABLE_ID.value
)


@flow(name=_suprido_empenho_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_suprido_empenho_check_update() -> (
    None
):
    _suprido_empenho_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_suprido_empenho_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_suprido_empenho.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_suprido_empenho_check_update.deploy_schedules = [
    Cron("48 12 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_suprido_empenho_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_suprido_empenho_download(
    download_params: dict,
) -> None:
    _suprido_empenho_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_suprido_empenho_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_suprido_empenho.TABLE_ID.value,
)
_suprido_empenho_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_suprido_empenho_download.fn.__name__


_suprido_transacao_pipeline = make_pipeline(
    constants_suprido_transacao.TABLE_ID.value
)


@flow(name=_suprido_transacao_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_suprido_transacao_check_update() -> (
    None
):
    _suprido_transacao_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_suprido_transacao_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_suprido_transacao.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_suprido_transacao_check_update.deploy_schedules = [
    Cron("53 12 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_suprido_transacao_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_suprido_transacao_download(
    download_params: dict,
) -> None:
    _suprido_transacao_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_suprido_transacao_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_suprido_transacao.TABLE_ID.value,
)
_suprido_transacao_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_suprido_transacao_download.fn.__name__


_suprido_transacao_objeto_pipeline = make_pipeline(
    constants_suprido_transacao_objeto.TABLE_ID.value
)


@flow(
    name=_suprido_transacao_objeto_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_suprido_transacao_objeto_check_update() -> (
    None
):
    _suprido_transacao_objeto_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_suprido_transacao_objeto_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_suprido_transacao_objeto.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_suprido_transacao_objeto_check_update.deploy_schedules = [
    Cron("58 12 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_suprido_transacao_objeto_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_suprido_transacao_objeto_download(
    download_params: dict,
) -> None:
    _suprido_transacao_objeto_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_suprido_transacao_objeto_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_suprido_transacao_objeto.TABLE_ID.value,
)
_suprido_transacao_objeto_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_suprido_transacao_objeto_download.fn.__name__


_quadro_pessoal_pipeline = make_pipeline(
    constants_quadro_pessoal.TABLE_ID.value
)


@flow(name=_quadro_pessoal_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_quadro_pessoal_check_update() -> (
    None
):
    _quadro_pessoal_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_quadro_pessoal_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_quadro_pessoal.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_quadro_pessoal_check_update.deploy_schedules = [
    Cron("3 13 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_quadro_pessoal_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_quadro_pessoal_download(
    download_params: dict,
) -> None:
    _quadro_pessoal_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_quadro_pessoal_download.deploy_tags = (
    deploy_tags(
        constants.DATASET_ID.value,
        Etapa.EXTRACT_AND_LOAD,
        constants_quadro_pessoal.TABLE_ID.value,
    )
)
_quadro_pessoal_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_administrativos_quadro_pessoal_download.fn.__name__
)


_diretor_coordenador_pipeline = make_pipeline(
    constants_diretor_coordenador.TABLE_ID.value
)


@flow(
    name=_diretor_coordenador_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_diretor_coordenador_check_update() -> (
    None
):
    _diretor_coordenador_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_diretor_coordenador_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_diretor_coordenador.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_diretor_coordenador_check_update.deploy_schedules = [
    Cron("8 13 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_diretor_coordenador_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_diretor_coordenador_download(
    download_params: dict,
) -> None:
    _diretor_coordenador_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_diretor_coordenador_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_diretor_coordenador.TABLE_ID.value,
)
_diretor_coordenador_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_diretor_coordenador_download.fn.__name__


_previsao_aposentadoria_pipeline = make_pipeline(
    constants_previsao_aposentadoria.TABLE_ID.value
)


@flow(
    name=_previsao_aposentadoria_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_previsao_aposentadoria_check_update() -> (
    None
):
    _previsao_aposentadoria_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_previsao_aposentadoria_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_previsao_aposentadoria.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_previsao_aposentadoria_check_update.deploy_schedules = [
    Cron("13 13 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_previsao_aposentadoria_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_previsao_aposentadoria_download(
    download_params: dict,
) -> None:
    _previsao_aposentadoria_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_previsao_aposentadoria_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_previsao_aposentadoria.TABLE_ID.value,
)
_previsao_aposentadoria_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_previsao_aposentadoria_download.fn.__name__


_contratacao_item_pipeline = make_pipeline(
    constants_contratacao_item.TABLE_ID.value
)


@flow(name=_contratacao_item_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_contratacao_item_check_update() -> (
    None
):
    _contratacao_item_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_contratacao_item_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_contratacao_item.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_contratacao_item_check_update.deploy_schedules = [
    Cron("18 13 * * 1", timezone="America/Sao_Paulo")
]


@flow(
    name=_contratacao_item_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_contratacao_item_download(
    download_params: dict,
) -> None:
    _contratacao_item_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_contratacao_item_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_contratacao_item.TABLE_ID.value,
)
_contratacao_item_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_contratacao_item_download.fn.__name__


_contratacao_garantia_pipeline = make_pipeline(
    constants_contratacao_garantia.TABLE_ID.value
)


@flow(
    name=_contratacao_garantia_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_contratacao_garantia_check_update() -> (
    None
):
    _contratacao_garantia_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_contratacao_garantia_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_contratacao_garantia.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_contratacao_garantia_check_update.deploy_schedules = [
    Cron("18 13 * * 2", timezone="America/Sao_Paulo")
]


@flow(
    name=_contratacao_garantia_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_contratacao_garantia_download(
    download_params: dict,
) -> None:
    _contratacao_garantia_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_contratacao_garantia_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_contratacao_garantia.TABLE_ID.value,
)
_contratacao_garantia_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_contratacao_garantia_download.fn.__name__


_contratacao_pagamento_pipeline = make_pipeline(
    constants_contratacao_pagamento.TABLE_ID.value
)


@flow(
    name=_contratacao_pagamento_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_contratacao_pagamento_check_update() -> (
    None
):
    _contratacao_pagamento_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_contratacao_pagamento_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_contratacao_pagamento.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_contratacao_pagamento_check_update.deploy_schedules = [
    Cron("18 13 * * 3", timezone="America/Sao_Paulo")
]


@flow(
    name=_contratacao_pagamento_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_contratacao_pagamento_download(
    download_params: dict,
) -> None:
    _contratacao_pagamento_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_contratacao_pagamento_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_contratacao_pagamento.TABLE_ID.value,
)
_contratacao_pagamento_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_contratacao_pagamento_download.fn.__name__


_contratacao_documento_fiscal_pipeline = make_pipeline(
    constants_contratacao_documento_fiscal.TABLE_ID.value
)


@flow(
    name=_contratacao_documento_fiscal_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_contratacao_documento_fiscal_check_update() -> (
    None
):
    _contratacao_documento_fiscal_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_contratacao_documento_fiscal_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_contratacao_documento_fiscal.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_contratacao_documento_fiscal_check_update.deploy_schedules = [
    Cron("18 13 * * 4", timezone="America/Sao_Paulo")
]


@flow(
    name=_contratacao_documento_fiscal_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_contratacao_documento_fiscal_download(
    download_params: dict,
) -> None:
    _contratacao_documento_fiscal_pipeline.run_extract_and_load(
        download_params
    )


br_senado_dados_abertos_administrativos_contratacao_documento_fiscal_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_contratacao_documento_fiscal.TABLE_ID.value,
)
_contratacao_documento_fiscal_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_contratacao_documento_fiscal_download.fn.__name__


_contratacao_pagamento_empenho_pipeline = make_pipeline(
    constants_contratacao_pagamento_empenho.TABLE_ID.value
)


@flow(
    name=_contratacao_pagamento_empenho_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_contratacao_pagamento_empenho_check_update() -> (
    None
):
    _contratacao_pagamento_empenho_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_contratacao_pagamento_empenho_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_contratacao_pagamento_empenho.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_contratacao_pagamento_empenho_check_update.deploy_schedules = [
    Cron("18 13 * * 5", timezone="America/Sao_Paulo")
]


@flow(
    name=_contratacao_pagamento_empenho_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_contratacao_pagamento_empenho_download(
    download_params: dict,
) -> None:
    _contratacao_pagamento_empenho_pipeline.run_extract_and_load(
        download_params
    )


br_senado_dados_abertos_administrativos_contratacao_pagamento_empenho_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_contratacao_pagamento_empenho.TABLE_ID.value,
)
_contratacao_pagamento_empenho_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_contratacao_pagamento_empenho_download.fn.__name__


_contrato_aditivo_pipeline = make_pipeline(
    constants_contrato_aditivo.TABLE_ID.value
)


@flow(name=_contrato_aditivo_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_contrato_aditivo_check_update() -> (
    None
):
    _contrato_aditivo_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_contrato_aditivo_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_contrato_aditivo.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_contrato_aditivo_check_update.deploy_schedules = [
    Cron("18 13 * * 6", timezone="America/Sao_Paulo")
]


@flow(
    name=_contrato_aditivo_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_contrato_aditivo_download(
    download_params: dict,
) -> None:
    _contrato_aditivo_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_contrato_aditivo_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_contrato_aditivo.TABLE_ID.value,
)
_contrato_aditivo_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_contrato_aditivo_download.fn.__name__


_ata_acionamento_pipeline = make_pipeline(
    constants_ata_acionamento.TABLE_ID.value
)


@flow(name=_ata_acionamento_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_administrativos_ata_acionamento_check_update() -> (
    None
):
    _ata_acionamento_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_ata_acionamento_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_ata_acionamento.TABLE_ID.value,
)
br_senado_dados_abertos_administrativos_ata_acionamento_check_update.deploy_schedules = [
    Cron("18 13 * * 0", timezone="America/Sao_Paulo")
]


@flow(
    name=_ata_acionamento_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_ata_acionamento_download(
    download_params: dict,
) -> None:
    _ata_acionamento_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_ata_acionamento_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_ata_acionamento.TABLE_ID.value,
)
_ata_acionamento_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_ata_acionamento_download.fn.__name__


_suprido_movimentacao_pipeline = make_pipeline(
    constants_suprido_movimentacao.TABLE_ID.value
)


@flow(
    name=_suprido_movimentacao_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_suprido_movimentacao_check_update() -> (
    None
):
    _suprido_movimentacao_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_suprido_movimentacao_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_suprido_movimentacao.TABLE_ID.value,
)


@flow(
    name=_suprido_movimentacao_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_suprido_movimentacao_download(
    download_params: dict,
) -> None:
    _suprido_movimentacao_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_administrativos_suprido_movimentacao_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_suprido_movimentacao.TABLE_ID.value,
)
_suprido_movimentacao_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_suprido_movimentacao_download.fn.__name__


_suprido_movimentacao_subtipo_pipeline = make_pipeline(
    constants_suprido_movimentacao_subtipo.TABLE_ID.value
)


@flow(
    name=_suprido_movimentacao_subtipo_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_suprido_movimentacao_subtipo_check_update() -> (
    None
):
    _suprido_movimentacao_subtipo_pipeline.run_check_update()


br_senado_dados_abertos_administrativos_suprido_movimentacao_subtipo_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_suprido_movimentacao_subtipo.TABLE_ID.value,
)


@flow(
    name=_suprido_movimentacao_subtipo_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_administrativos_suprido_movimentacao_subtipo_download(
    download_params: dict,
) -> None:
    _suprido_movimentacao_subtipo_pipeline.run_extract_and_load(
        download_params
    )


br_senado_dados_abertos_administrativos_suprido_movimentacao_subtipo_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_suprido_movimentacao_subtipo.TABLE_ID.value,
)
_suprido_movimentacao_subtipo_pipeline.extract_load_deployment = br_senado_dados_abertos_administrativos_suprido_movimentacao_subtipo_download.fn.__name__
