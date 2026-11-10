"""Flows de br_senado_dados_abertos — Prefect 3, modelo em estágios."""

from prefect.schedules import Cron

from pipelines.datasets.br_senado_dados_abertos.constants import (
    constants,
    constants_bloco,
    constants_comissao,
    constants_discurso,
    constants_lideranca,
    constants_mesa,
    constants_partido,
    constants_processo,
    constants_relatoria,
    constants_senador,
    constants_senador_cargo,
    constants_senador_comissao,
    constants_senador_filiacao,
    constants_senador_mandato,
    constants_votacao,
    constants_votacao_comissao,
    constants_votacao_comissao_parlamentar,
    constants_votacao_orientacao_bancada,
    constants_votacao_parlamentar,
)
from pipelines.datasets.br_senado_dados_abertos.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

_senador_pipeline = make_pipeline(constants_senador.TABLE_ID.value)


@flow(name=_senador_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_senador_check_update() -> None:
    _senador_pipeline.run_check_update()


br_senado_dados_abertos_senador_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_senador.TABLE_ID.value,
)
br_senado_dados_abertos_senador_check_update.deploy_schedules = [
    Cron("15 8 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_senador_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_senador_download(download_params: dict) -> None:
    _senador_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_senador_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_senador.TABLE_ID.value,
)
_senador_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_senador_download.fn.__name__
)


_partido_pipeline = make_pipeline(constants_partido.TABLE_ID.value)


@flow(name=_partido_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_partido_check_update() -> None:
    _partido_pipeline.run_check_update()


br_senado_dados_abertos_partido_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_partido.TABLE_ID.value,
)
br_senado_dados_abertos_partido_check_update.deploy_schedules = [
    Cron("23 8 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_partido_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_partido_download(download_params: dict) -> None:
    _partido_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_partido_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_partido.TABLE_ID.value,
)
_partido_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_partido_download.fn.__name__
)


_bloco_pipeline = make_pipeline(constants_bloco.TABLE_ID.value)


@flow(name=_bloco_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_bloco_check_update() -> None:
    _bloco_pipeline.run_check_update()


br_senado_dados_abertos_bloco_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_bloco.TABLE_ID.value,
)
br_senado_dados_abertos_bloco_check_update.deploy_schedules = [
    Cron("31 8 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_bloco_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_bloco_download(download_params: dict) -> None:
    _bloco_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_bloco_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_bloco.TABLE_ID.value,
)
_bloco_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_bloco_download.fn.__name__
)


_lideranca_pipeline = make_pipeline(constants_lideranca.TABLE_ID.value)


@flow(name=_lideranca_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_lideranca_check_update() -> None:
    _lideranca_pipeline.run_check_update()


br_senado_dados_abertos_lideranca_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_lideranca.TABLE_ID.value,
)
br_senado_dados_abertos_lideranca_check_update.deploy_schedules = [
    Cron("39 8 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_lideranca_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_lideranca_download(download_params: dict) -> None:
    _lideranca_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_lideranca_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_lideranca.TABLE_ID.value,
)
_lideranca_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_lideranca_download.fn.__name__
)


_comissao_pipeline = make_pipeline(constants_comissao.TABLE_ID.value)


@flow(name=_comissao_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_comissao_check_update() -> None:
    _comissao_pipeline.run_check_update()


br_senado_dados_abertos_comissao_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_comissao.TABLE_ID.value,
)
br_senado_dados_abertos_comissao_check_update.deploy_schedules = [
    Cron("47 8 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_comissao_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_comissao_download(download_params: dict) -> None:
    _comissao_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_comissao_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_comissao.TABLE_ID.value,
)
_comissao_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_comissao_download.fn.__name__
)


_mesa_pipeline = make_pipeline(constants_mesa.TABLE_ID.value)


@flow(name=_mesa_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_mesa_check_update() -> None:
    _mesa_pipeline.run_check_update()


br_senado_dados_abertos_mesa_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_mesa.TABLE_ID.value,
)
br_senado_dados_abertos_mesa_check_update.deploy_schedules = [
    Cron("55 8 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_mesa_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_mesa_download(download_params: dict) -> None:
    _mesa_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_mesa_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_mesa.TABLE_ID.value,
)
_mesa_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_mesa_download.fn.__name__
)


_senador_mandato_pipeline = make_pipeline(
    constants_senador_mandato.TABLE_ID.value
)


@flow(name=_senador_mandato_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_senador_mandato_check_update() -> None:
    _senador_mandato_pipeline.run_check_update()


br_senado_dados_abertos_senador_mandato_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_senador_mandato.TABLE_ID.value,
)
br_senado_dados_abertos_senador_mandato_check_update.deploy_schedules = [
    Cron("3 9 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_senador_mandato_pipeline.extract_and_load_flow_name, log_prints=True
)
def br_senado_dados_abertos_senador_mandato_download(
    download_params: dict,
) -> None:
    _senador_mandato_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_senador_mandato_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_senador_mandato.TABLE_ID.value,
)
_senador_mandato_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_senador_mandato_download.fn.__name__
)


_senador_filiacao_pipeline = make_pipeline(
    constants_senador_filiacao.TABLE_ID.value
)


@flow(name=_senador_filiacao_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_senador_filiacao_check_update() -> None:
    _senador_filiacao_pipeline.run_check_update()


br_senado_dados_abertos_senador_filiacao_check_update.deploy_tags = (
    deploy_tags(
        constants.DATASET_ID.value,
        Etapa.CHECK_UPDATE,
        constants_senador_filiacao.TABLE_ID.value,
    )
)
br_senado_dados_abertos_senador_filiacao_check_update.deploy_schedules = [
    Cron("11 9 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_senador_filiacao_pipeline.extract_and_load_flow_name, log_prints=True
)
def br_senado_dados_abertos_senador_filiacao_download(
    download_params: dict,
) -> None:
    _senador_filiacao_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_senador_filiacao_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_senador_filiacao.TABLE_ID.value,
)
_senador_filiacao_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_senador_filiacao_download.fn.__name__
)


_senador_comissao_pipeline = make_pipeline(
    constants_senador_comissao.TABLE_ID.value
)


@flow(name=_senador_comissao_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_senador_comissao_check_update() -> None:
    _senador_comissao_pipeline.run_check_update()


br_senado_dados_abertos_senador_comissao_check_update.deploy_tags = (
    deploy_tags(
        constants.DATASET_ID.value,
        Etapa.CHECK_UPDATE,
        constants_senador_comissao.TABLE_ID.value,
    )
)
br_senado_dados_abertos_senador_comissao_check_update.deploy_schedules = [
    Cron("19 9 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_senador_comissao_pipeline.extract_and_load_flow_name, log_prints=True
)
def br_senado_dados_abertos_senador_comissao_download(
    download_params: dict,
) -> None:
    _senador_comissao_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_senador_comissao_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_senador_comissao.TABLE_ID.value,
)
_senador_comissao_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_senador_comissao_download.fn.__name__
)


_senador_cargo_pipeline = make_pipeline(constants_senador_cargo.TABLE_ID.value)


@flow(name=_senador_cargo_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_senador_cargo_check_update() -> None:
    _senador_cargo_pipeline.run_check_update()


br_senado_dados_abertos_senador_cargo_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_senador_cargo.TABLE_ID.value,
)
br_senado_dados_abertos_senador_cargo_check_update.deploy_schedules = [
    Cron("27 9 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_senador_cargo_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_senador_cargo_download(
    download_params: dict,
) -> None:
    _senador_cargo_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_senador_cargo_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_senador_cargo.TABLE_ID.value,
)
_senador_cargo_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_senador_cargo_download.fn.__name__
)


_votacao_pipeline = make_pipeline(constants_votacao.TABLE_ID.value)


@flow(name=_votacao_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_votacao_check_update() -> None:
    _votacao_pipeline.run_check_update()


br_senado_dados_abertos_votacao_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_votacao.TABLE_ID.value,
)
br_senado_dados_abertos_votacao_check_update.deploy_schedules = [
    Cron("35 9 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_votacao_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_votacao_download(download_params: dict) -> None:
    _votacao_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_votacao_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_votacao.TABLE_ID.value,
)
_votacao_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_votacao_download.fn.__name__
)


_votacao_parlamentar_pipeline = make_pipeline(
    constants_votacao_parlamentar.TABLE_ID.value
)


@flow(
    name=_votacao_parlamentar_pipeline.check_update_flow_name, log_prints=True
)
def br_senado_dados_abertos_votacao_parlamentar_check_update() -> None:
    _votacao_parlamentar_pipeline.run_check_update()


br_senado_dados_abertos_votacao_parlamentar_check_update.deploy_tags = (
    deploy_tags(
        constants.DATASET_ID.value,
        Etapa.CHECK_UPDATE,
        constants_votacao_parlamentar.TABLE_ID.value,
    )
)
br_senado_dados_abertos_votacao_parlamentar_check_update.deploy_schedules = [
    Cron("43 9 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_votacao_parlamentar_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_votacao_parlamentar_download(
    download_params: dict,
) -> None:
    _votacao_parlamentar_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_votacao_parlamentar_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_votacao_parlamentar.TABLE_ID.value,
)
_votacao_parlamentar_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_votacao_parlamentar_download.fn.__name__
)


_votacao_orientacao_bancada_pipeline = make_pipeline(
    constants_votacao_orientacao_bancada.TABLE_ID.value
)


@flow(
    name=_votacao_orientacao_bancada_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_votacao_orientacao_bancada_check_update() -> None:
    _votacao_orientacao_bancada_pipeline.run_check_update()


br_senado_dados_abertos_votacao_orientacao_bancada_check_update.deploy_tags = (
    deploy_tags(
        constants.DATASET_ID.value,
        Etapa.CHECK_UPDATE,
        constants_votacao_orientacao_bancada.TABLE_ID.value,
    )
)
br_senado_dados_abertos_votacao_orientacao_bancada_check_update.deploy_schedules = [
    Cron("51 9 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_votacao_orientacao_bancada_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_votacao_orientacao_bancada_download(
    download_params: dict,
) -> None:
    _votacao_orientacao_bancada_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_votacao_orientacao_bancada_download.deploy_tags = (
    deploy_tags(
        constants.DATASET_ID.value,
        Etapa.EXTRACT_AND_LOAD,
        constants_votacao_orientacao_bancada.TABLE_ID.value,
    )
)
_votacao_orientacao_bancada_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_votacao_orientacao_bancada_download.fn.__name__
)


_processo_pipeline = make_pipeline(constants_processo.TABLE_ID.value)


@flow(name=_processo_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_processo_check_update() -> None:
    _processo_pipeline.run_check_update()


br_senado_dados_abertos_processo_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_processo.TABLE_ID.value,
)
br_senado_dados_abertos_processo_check_update.deploy_schedules = [
    Cron("59 9 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_processo_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_processo_download(download_params: dict) -> None:
    _processo_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_processo_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_processo.TABLE_ID.value,
)
_processo_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_processo_download.fn.__name__
)


_relatoria_pipeline = make_pipeline(constants_relatoria.TABLE_ID.value)


@flow(name=_relatoria_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_relatoria_check_update() -> None:
    _relatoria_pipeline.run_check_update()


br_senado_dados_abertos_relatoria_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_relatoria.TABLE_ID.value,
)
br_senado_dados_abertos_relatoria_check_update.deploy_schedules = [
    Cron("7 10 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_relatoria_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_relatoria_download(download_params: dict) -> None:
    _relatoria_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_relatoria_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_relatoria.TABLE_ID.value,
)
_relatoria_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_relatoria_download.fn.__name__
)


_votacao_comissao_pipeline = make_pipeline(
    constants_votacao_comissao.TABLE_ID.value
)


@flow(name=_votacao_comissao_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_votacao_comissao_check_update() -> None:
    _votacao_comissao_pipeline.run_check_update()


br_senado_dados_abertos_votacao_comissao_check_update.deploy_tags = (
    deploy_tags(
        constants.DATASET_ID.value,
        Etapa.CHECK_UPDATE,
        constants_votacao_comissao.TABLE_ID.value,
    )
)
br_senado_dados_abertos_votacao_comissao_check_update.deploy_schedules = [
    Cron("15 10 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_votacao_comissao_pipeline.extract_and_load_flow_name, log_prints=True
)
def br_senado_dados_abertos_votacao_comissao_download(
    download_params: dict,
) -> None:
    _votacao_comissao_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_votacao_comissao_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_votacao_comissao.TABLE_ID.value,
)
_votacao_comissao_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_votacao_comissao_download.fn.__name__
)


_votacao_comissao_parlamentar_pipeline = make_pipeline(
    constants_votacao_comissao_parlamentar.TABLE_ID.value
)


@flow(
    name=_votacao_comissao_parlamentar_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_votacao_comissao_parlamentar_check_update() -> (
    None
):
    _votacao_comissao_parlamentar_pipeline.run_check_update()


br_senado_dados_abertos_votacao_comissao_parlamentar_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_votacao_comissao_parlamentar.TABLE_ID.value,
)
br_senado_dados_abertos_votacao_comissao_parlamentar_check_update.deploy_schedules = [
    Cron("23 10 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_votacao_comissao_parlamentar_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_votacao_comissao_parlamentar_download(
    download_params: dict,
) -> None:
    _votacao_comissao_parlamentar_pipeline.run_extract_and_load(
        download_params
    )


br_senado_dados_abertos_votacao_comissao_parlamentar_download.deploy_tags = (
    deploy_tags(
        constants.DATASET_ID.value,
        Etapa.EXTRACT_AND_LOAD,
        constants_votacao_comissao_parlamentar.TABLE_ID.value,
    )
)
_votacao_comissao_parlamentar_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_votacao_comissao_parlamentar_download.fn.__name__
)


_discurso_pipeline = make_pipeline(constants_discurso.TABLE_ID.value)


@flow(name=_discurso_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_discurso_check_update() -> None:
    _discurso_pipeline.run_check_update()


br_senado_dados_abertos_discurso_check_update.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    constants_discurso.TABLE_ID.value,
)
br_senado_dados_abertos_discurso_check_update.deploy_schedules = [
    Cron("31 10 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_discurso_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_discurso_download(download_params: dict) -> None:
    _discurso_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_discurso_download.deploy_tags = deploy_tags(
    constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    constants_discurso.TABLE_ID.value,
)
_discurso_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_discurso_download.fn.__name__
)
