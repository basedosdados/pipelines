"""
Flows para br_cgu_servidores_executivo_federal — Prefect 3.

Migrado por completo pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote, uma dupla de flows por
tabela. Lógica específica do dataset mora em `tasks.py`, constantes em
`constants.py` — aqui só a fiação (`CheckThenExtractLoadPipeline` + `@flow`).

Cada tabela combina um ou mais pacotes/subsistemas do Portal da Transparência
(SIAPE, BACEN, Militares, DEFESA, Reserva/Reforma — ver README.md). As 7
tabelas baixam de forma independente — sem dependência real de ordem entre
elas no dbt (nenhum `ref()` cruzado entre as próprias tabelas do dataset em
`models/br_cgu_servidores_executivo_federal/*.sql`) — por isso 7 pares
check_update/extract_and_load independentes, sem materialização em lote.

Disponibilidade "tudo-ou-nada": o Portal publica os subsistemas em datas
escalonadas dentro do mês. `get_latest_update` (`tasks.py`) só avança
`reference_date` quando todos os subsistemas da tabela já estão publicados;
se faltar algum, mantém a data já conhecida pelo backend — o poll não vê
novidade e o extract_and_load não é disparado nesse ciclo.

O antigo `_run_cgu_servidores_publicos` monolítico segue existindo em
`pipelines/shared/cgu/flows.py`, compartilhado com outros datasets br_cgu_*
ainda não migrados (`br_cgu_licitacao_contrato`) — não removido daqui.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_cgu_servidores_executivo_federal.constants import (
    AFASTAMENTOS_TABLE_ID,
    CADASTRO_APOSENTADOS_TABLE_ID,
    CADASTRO_PENSIONISTAS_TABLE_ID,
    CADASTRO_RESERVA_REFORMA_MILITARES_TABLE_ID,
    CADASTRO_SERVIDORES_TABLE_ID,
    DATASET_ID,
    OBSERVACOES_TABLE_ID,
    REMUNERACAO_TABLE_ID,
)
from pipelines.datasets.br_cgu_servidores_executivo_federal.tasks import (
    make_pipeline,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# ──────────────────────────────────────────────────────────────────────────────
# cadastro_aposentados
# check_update: br_cgu_servidores_executivo_federal__cadastro_aposentados
# extract_and_load: br_cgu_servidores_executivo_federal__cadastro_aposentados
# ──────────────────────────────────────────────────────────────────────────────

_cadastro_aposentados_pipeline = make_pipeline(CADASTRO_APOSENTADOS_TABLE_ID)


@flow(
    name=_cadastro_aposentados_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cgu_servidores_executivo_federal_cadastro_aposentados_check_update() -> (
    None
):
    _cadastro_aposentados_pipeline.run_check_update()


br_cgu_servidores_executivo_federal_cadastro_aposentados_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, CADASTRO_APOSENTADOS_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_cgu_servidores_executivo_federal_cadastro_aposentados_check_update.deploy_schedules = [
    Cron("0 6 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_cadastro_aposentados_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cgu_servidores_executivo_federal_cadastro_aposentados_download(
    download_params: dict,
) -> None:
    _cadastro_aposentados_pipeline.run_extract_and_load(download_params)


br_cgu_servidores_executivo_federal_cadastro_aposentados_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, CADASTRO_APOSENTADOS_TABLE_ID
)
_cadastro_aposentados_pipeline.extract_load_deployment = br_cgu_servidores_executivo_federal_cadastro_aposentados_download.fn.__name__


# ──────────────────────────────────────────────────────────────────────────────
# cadastro_pensionistas
# check_update: br_cgu_servidores_executivo_federal__cadastro_pensionistas
# extract_and_load: br_cgu_servidores_executivo_federal__cadastro_pensionistas
# ──────────────────────────────────────────────────────────────────────────────

_cadastro_pensionistas_pipeline = make_pipeline(CADASTRO_PENSIONISTAS_TABLE_ID)


@flow(
    name=_cadastro_pensionistas_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cgu_servidores_executivo_federal_cadastro_pensionistas_check_update() -> (
    None
):
    _cadastro_pensionistas_pipeline.run_check_update()


br_cgu_servidores_executivo_federal_cadastro_pensionistas_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, CADASTRO_PENSIONISTAS_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_cgu_servidores_executivo_federal_cadastro_pensionistas_check_update.deploy_schedules = [
    Cron("15 6 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_cadastro_pensionistas_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cgu_servidores_executivo_federal_cadastro_pensionistas_download(
    download_params: dict,
) -> None:
    _cadastro_pensionistas_pipeline.run_extract_and_load(download_params)


br_cgu_servidores_executivo_federal_cadastro_pensionistas_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, CADASTRO_PENSIONISTAS_TABLE_ID
)
_cadastro_pensionistas_pipeline.extract_load_deployment = br_cgu_servidores_executivo_federal_cadastro_pensionistas_download.fn.__name__


# ──────────────────────────────────────────────────────────────────────────────
# cadastro_servidores
# check_update: br_cgu_servidores_executivo_federal__cadastro_servidores
# extract_and_load: br_cgu_servidores_executivo_federal__cadastro_servidores
# ──────────────────────────────────────────────────────────────────────────────

_cadastro_servidores_pipeline = make_pipeline(CADASTRO_SERVIDORES_TABLE_ID)


@flow(
    name=_cadastro_servidores_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cgu_servidores_executivo_federal_cadastro_servidores_check_update() -> (
    None
):
    _cadastro_servidores_pipeline.run_check_update()


br_cgu_servidores_executivo_federal_cadastro_servidores_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, CADASTRO_SERVIDORES_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_cgu_servidores_executivo_federal_cadastro_servidores_check_update.deploy_schedules = [
    Cron("30 6 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_cadastro_servidores_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cgu_servidores_executivo_federal_cadastro_servidores_download(
    download_params: dict,
) -> None:
    _cadastro_servidores_pipeline.run_extract_and_load(download_params)


br_cgu_servidores_executivo_federal_cadastro_servidores_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, CADASTRO_SERVIDORES_TABLE_ID
)
_cadastro_servidores_pipeline.extract_load_deployment = br_cgu_servidores_executivo_federal_cadastro_servidores_download.fn.__name__


# ──────────────────────────────────────────────────────────────────────────────
# cadastro_reserva_reforma_militares
# check_update: br_cgu_servidores_executivo_federal__cadastro_reserva_reforma_militares
# extract_and_load: br_cgu_servidores_executivo_federal__cadastro_reserva_reforma_militares
# ──────────────────────────────────────────────────────────────────────────────

_cadastro_reserva_reforma_militares_pipeline = make_pipeline(
    CADASTRO_RESERVA_REFORMA_MILITARES_TABLE_ID
)


@flow(
    name=_cadastro_reserva_reforma_militares_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cgu_servidores_executivo_federal_cadastro_reserva_reforma_militares_check_update() -> (
    None
):
    _cadastro_reserva_reforma_militares_pipeline.run_check_update()


br_cgu_servidores_executivo_federal_cadastro_reserva_reforma_militares_check_update.deploy_tags = deploy_tags(
    DATASET_ID,
    Etapa.CHECK_UPDATE,
    CADASTRO_RESERVA_REFORMA_MILITARES_TABLE_ID,
)
# Mesmo cron do flow monolítico antigo (main).
br_cgu_servidores_executivo_federal_cadastro_reserva_reforma_militares_check_update.deploy_schedules = [
    Cron("45 6 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_cadastro_reserva_reforma_militares_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cgu_servidores_executivo_federal_cadastro_reserva_reforma_militares_download(
    download_params: dict,
) -> None:
    _cadastro_reserva_reforma_militares_pipeline.run_extract_and_load(
        download_params
    )


br_cgu_servidores_executivo_federal_cadastro_reserva_reforma_militares_download.deploy_tags = deploy_tags(
    DATASET_ID,
    Etapa.EXTRACT_AND_LOAD,
    CADASTRO_RESERVA_REFORMA_MILITARES_TABLE_ID,
)
_cadastro_reserva_reforma_militares_pipeline.extract_load_deployment = br_cgu_servidores_executivo_federal_cadastro_reserva_reforma_militares_download.fn.__name__


# ──────────────────────────────────────────────────────────────────────────────
# remuneracao
# check_update: br_cgu_servidores_executivo_federal__remuneracao
# extract_and_load: br_cgu_servidores_executivo_federal__remuneracao
# ──────────────────────────────────────────────────────────────────────────────

_remuneracao_pipeline = make_pipeline(REMUNERACAO_TABLE_ID)


@flow(
    name=_remuneracao_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cgu_servidores_executivo_federal_remuneracao_check_update() -> None:
    _remuneracao_pipeline.run_check_update()


br_cgu_servidores_executivo_federal_remuneracao_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, REMUNERACAO_TABLE_ID)
)
# Mesmo cron do flow monolítico antigo (main).
br_cgu_servidores_executivo_federal_remuneracao_check_update.deploy_schedules = [
    Cron("0 7 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_remuneracao_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cgu_servidores_executivo_federal_remuneracao_download(
    download_params: dict,
) -> None:
    _remuneracao_pipeline.run_extract_and_load(download_params)


br_cgu_servidores_executivo_federal_remuneracao_download.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.EXTRACT_AND_LOAD, REMUNERACAO_TABLE_ID)
)
_remuneracao_pipeline.extract_load_deployment = (
    br_cgu_servidores_executivo_federal_remuneracao_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# afastamentos
# check_update: br_cgu_servidores_executivo_federal__afastamentos
# extract_and_load: br_cgu_servidores_executivo_federal__afastamentos
# ──────────────────────────────────────────────────────────────────────────────

_afastamentos_pipeline = make_pipeline(AFASTAMENTOS_TABLE_ID)


@flow(
    name=_afastamentos_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cgu_servidores_executivo_federal_afastamentos_check_update() -> None:
    _afastamentos_pipeline.run_check_update()


br_cgu_servidores_executivo_federal_afastamentos_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, AFASTAMENTOS_TABLE_ID)
)
# Mesmo cron do flow monolítico antigo (main).
br_cgu_servidores_executivo_federal_afastamentos_check_update.deploy_schedules = [
    Cron("15 7 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_afastamentos_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cgu_servidores_executivo_federal_afastamentos_download(
    download_params: dict,
) -> None:
    _afastamentos_pipeline.run_extract_and_load(download_params)


br_cgu_servidores_executivo_federal_afastamentos_download.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.EXTRACT_AND_LOAD, AFASTAMENTOS_TABLE_ID)
)
_afastamentos_pipeline.extract_load_deployment = (
    br_cgu_servidores_executivo_federal_afastamentos_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# observacoes
# check_update: br_cgu_servidores_executivo_federal__observacoes
# extract_and_load: br_cgu_servidores_executivo_federal__observacoes
# ──────────────────────────────────────────────────────────────────────────────

_observacoes_pipeline = make_pipeline(OBSERVACOES_TABLE_ID)


@flow(
    name=_observacoes_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cgu_servidores_executivo_federal_observacoes_check_update() -> None:
    _observacoes_pipeline.run_check_update()


br_cgu_servidores_executivo_federal_observacoes_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, OBSERVACOES_TABLE_ID)
)
# Mesmo cron do flow monolítico antigo (main).
br_cgu_servidores_executivo_federal_observacoes_check_update.deploy_schedules = [
    Cron("30 7 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_observacoes_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cgu_servidores_executivo_federal_observacoes_download(
    download_params: dict,
) -> None:
    _observacoes_pipeline.run_extract_and_load(download_params)


br_cgu_servidores_executivo_federal_observacoes_download.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.EXTRACT_AND_LOAD, OBSERVACOES_TABLE_ID)
)
_observacoes_pipeline.extract_load_deployment = (
    br_cgu_servidores_executivo_federal_observacoes_download.fn.__name__
)
