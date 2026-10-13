"""
Flows para br_cgu_licitacao_contrato — Prefect 3.

Migrado por completo pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote, uma dupla de flows por
tabela. Lógica específica do dataset mora em `tasks.py`, constantes em
`constants.py` — aqui só a fiação (`CheckThenExtractLoadPipeline` + `@flow`).

A variante "check_and_download" (reaproveitar no extract_and_load o download
já feito pelo check_update) foi cancelada — todo dataset usa check_update ->
extract_and_load, mesmo quando o check precisa baixar dado pra descobrir a
data. Aqui, `get_current_date_and_download_file` baixa o ZIP do mês tanto no
check_update quanto no extract_and_load, de forma independente — custo
duplicado aceito (ver tasks.py).

7 tabelas — duas fontes (licitações e compras), cada ZIP mensal contém os
CSVs de várias tabelas do mesmo grupo — mas sem dependência real de ordem no
dbt (nenhum `ref()` cruzado entre elas em schema.yml/*.sql, só
`relationships` contra diretórios externos). Por isso uma dupla de flows por
tabela, repetindo download/parse quando necessário (regra de ouro do
pipeline em estágios), sem materialização em lote.

O antigo `_run_cgu_licitacao_contrato` monolítico segue existindo em
`pipelines/crawler/cgu/flows.py`, compartilhado com
`br_cgu_cartao_pagamento`/`br_cgu_servidores_publicos`/
`br_cgu_beneficios_cidadao` (não removido daqui — os outros datasets da
família ainda dependem dele, ainda não migrados).
"""

from prefect.schedules import Cron

from pipelines.datasets.br_cgu_licitacao_contrato.constants import (
    CONTRATO_COMPRA_TABLE_ID,
    CONTRATO_ITEM_TABLE_ID,
    CONTRATO_TERMO_ADITIVO_TABLE_ID,
    DATASET_ID,
    LICITACAO_EMPENHO_TABLE_ID,
    LICITACAO_ITEM_TABLE_ID,
    LICITACAO_PARTICIPANTE_TABLE_ID,
    LICITACAO_TABLE_ID,
)
from pipelines.datasets.br_cgu_licitacao_contrato.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# ──────────────────────────────────────────────────────────────────────────────
# licitacao
# check_update: br_cgu_licitacao_contrato__licitacao
# extract_and_load: br_cgu_licitacao_contrato__licitacao
# ──────────────────────────────────────────────────────────────────────────────

_licitacao_pipeline = make_pipeline(LICITACAO_TABLE_ID)


@flow(name=_licitacao_pipeline.check_update_flow_name, log_prints=True)
def br_cgu_licitacao_contrato_licitacao_check_update() -> None:
    _licitacao_pipeline.run_check_update()


br_cgu_licitacao_contrato_licitacao_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, LICITACAO_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_cgu_licitacao_contrato_licitacao_check_update.deploy_schedules = [
    Cron("45 20 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_licitacao_pipeline.extract_and_load_flow_name, log_prints=True)
def br_cgu_licitacao_contrato_licitacao_download(
    download_params: dict,
) -> None:
    _licitacao_pipeline.run_extract_and_load(download_params)


br_cgu_licitacao_contrato_licitacao_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, LICITACAO_TABLE_ID
)
_licitacao_pipeline.extract_load_deployment = (
    br_cgu_licitacao_contrato_licitacao_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# licitacao_participante
# check_update: br_cgu_licitacao_contrato__licitacao_participante
# extract_and_load: br_cgu_licitacao_contrato__licitacao_participante
# ──────────────────────────────────────────────────────────────────────────────

_licitacao_participante_pipeline = make_pipeline(
    LICITACAO_PARTICIPANTE_TABLE_ID
)


@flow(
    name=_licitacao_participante_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cgu_licitacao_contrato_licitacao_participante_check_update() -> None:
    _licitacao_participante_pipeline.run_check_update()


br_cgu_licitacao_contrato_licitacao_participante_check_update.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.CHECK_UPDATE, LICITACAO_PARTICIPANTE_TABLE_ID
    )
)
# Mesmo cron do flow monolítico antigo (main).
br_cgu_licitacao_contrato_licitacao_participante_check_update.deploy_schedules = [
    Cron("35 20 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_licitacao_participante_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cgu_licitacao_contrato_licitacao_participante_download(
    download_params: dict,
) -> None:
    _licitacao_participante_pipeline.run_extract_and_load(download_params)


br_cgu_licitacao_contrato_licitacao_participante_download.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, LICITACAO_PARTICIPANTE_TABLE_ID
    )
)
_licitacao_participante_pipeline.extract_load_deployment = (
    br_cgu_licitacao_contrato_licitacao_participante_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# licitacao_item
# check_update: br_cgu_licitacao_contrato__licitacao_item
# extract_and_load: br_cgu_licitacao_contrato__licitacao_item
# ──────────────────────────────────────────────────────────────────────────────

_licitacao_item_pipeline = make_pipeline(LICITACAO_ITEM_TABLE_ID)


@flow(name=_licitacao_item_pipeline.check_update_flow_name, log_prints=True)
def br_cgu_licitacao_contrato_licitacao_item_check_update() -> None:
    _licitacao_item_pipeline.run_check_update()


br_cgu_licitacao_contrato_licitacao_item_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, LICITACAO_ITEM_TABLE_ID)
)
# Mesmo cron do flow monolítico antigo (main).
br_cgu_licitacao_contrato_licitacao_item_check_update.deploy_schedules = [
    Cron("20 20 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_licitacao_item_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cgu_licitacao_contrato_licitacao_item_download(
    download_params: dict,
) -> None:
    _licitacao_item_pipeline.run_extract_and_load(download_params)


br_cgu_licitacao_contrato_licitacao_item_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, LICITACAO_ITEM_TABLE_ID
)
_licitacao_item_pipeline.extract_load_deployment = (
    br_cgu_licitacao_contrato_licitacao_item_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# licitacao_empenho
# check_update: br_cgu_licitacao_contrato__licitacao_empenho
# extract_and_load: br_cgu_licitacao_contrato__licitacao_empenho
# ──────────────────────────────────────────────────────────────────────────────

_licitacao_empenho_pipeline = make_pipeline(LICITACAO_EMPENHO_TABLE_ID)


@flow(
    name=_licitacao_empenho_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cgu_licitacao_contrato_licitacao_empenho_check_update() -> None:
    _licitacao_empenho_pipeline.run_check_update()


br_cgu_licitacao_contrato_licitacao_empenho_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, LICITACAO_EMPENHO_TABLE_ID)
)
# Sem cron no flow monolítico antigo (main) — mantido sem schedule.


@flow(
    name=_licitacao_empenho_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cgu_licitacao_contrato_licitacao_empenho_download(
    download_params: dict,
) -> None:
    _licitacao_empenho_pipeline.run_extract_and_load(download_params)


br_cgu_licitacao_contrato_licitacao_empenho_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, LICITACAO_EMPENHO_TABLE_ID
)
_licitacao_empenho_pipeline.extract_load_deployment = (
    br_cgu_licitacao_contrato_licitacao_empenho_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# contrato_compra
# check_update: br_cgu_licitacao_contrato__contrato_compra
# extract_and_load: br_cgu_licitacao_contrato__contrato_compra
# ──────────────────────────────────────────────────────────────────────────────

_contrato_compra_pipeline = make_pipeline(CONTRATO_COMPRA_TABLE_ID)


@flow(name=_contrato_compra_pipeline.check_update_flow_name, log_prints=True)
def br_cgu_licitacao_contrato_contrato_compra_check_update() -> None:
    _contrato_compra_pipeline.run_check_update()


br_cgu_licitacao_contrato_contrato_compra_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, CONTRATO_COMPRA_TABLE_ID)
)
# Mesmo cron do flow monolítico antigo (main).
br_cgu_licitacao_contrato_contrato_compra_check_update.deploy_schedules = [
    Cron("0 21 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_contrato_compra_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cgu_licitacao_contrato_contrato_compra_download(
    download_params: dict,
) -> None:
    _contrato_compra_pipeline.run_extract_and_load(download_params)


br_cgu_licitacao_contrato_contrato_compra_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, CONTRATO_COMPRA_TABLE_ID
)
_contrato_compra_pipeline.extract_load_deployment = (
    br_cgu_licitacao_contrato_contrato_compra_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# contrato_item
# check_update: br_cgu_licitacao_contrato__contrato_item
# extract_and_load: br_cgu_licitacao_contrato__contrato_item
# ──────────────────────────────────────────────────────────────────────────────

_contrato_item_pipeline = make_pipeline(CONTRATO_ITEM_TABLE_ID)


@flow(name=_contrato_item_pipeline.check_update_flow_name, log_prints=True)
def br_cgu_licitacao_contrato_contrato_item_check_update() -> None:
    _contrato_item_pipeline.run_check_update()


br_cgu_licitacao_contrato_contrato_item_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, CONTRATO_ITEM_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_cgu_licitacao_contrato_contrato_item_check_update.deploy_schedules = [
    Cron("15 20 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_contrato_item_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cgu_licitacao_contrato_contrato_item_download(
    download_params: dict,
) -> None:
    _contrato_item_pipeline.run_extract_and_load(download_params)


br_cgu_licitacao_contrato_contrato_item_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, CONTRATO_ITEM_TABLE_ID
)
_contrato_item_pipeline.extract_load_deployment = (
    br_cgu_licitacao_contrato_contrato_item_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# contrato_termo_aditivo
# check_update: br_cgu_licitacao_contrato__contrato_termo_aditivo
# extract_and_load: br_cgu_licitacao_contrato__contrato_termo_aditivo
# ──────────────────────────────────────────────────────────────────────────────

_contrato_termo_aditivo_pipeline = make_pipeline(
    CONTRATO_TERMO_ADITIVO_TABLE_ID
)


@flow(
    name=_contrato_termo_aditivo_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cgu_licitacao_contrato_contrato_termo_aditivo_check_update() -> None:
    _contrato_termo_aditivo_pipeline.run_check_update()


br_cgu_licitacao_contrato_contrato_termo_aditivo_check_update.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.CHECK_UPDATE, CONTRATO_TERMO_ADITIVO_TABLE_ID
    )
)
# Mesmo cron do flow monolítico antigo (main).
br_cgu_licitacao_contrato_contrato_termo_aditivo_check_update.deploy_schedules = [
    Cron("30 20 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_contrato_termo_aditivo_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cgu_licitacao_contrato_contrato_termo_aditivo_download(
    download_params: dict,
) -> None:
    _contrato_termo_aditivo_pipeline.run_extract_and_load(download_params)


br_cgu_licitacao_contrato_contrato_termo_aditivo_download.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, CONTRATO_TERMO_ADITIVO_TABLE_ID
    )
)
_contrato_termo_aditivo_pipeline.extract_load_deployment = (
    br_cgu_licitacao_contrato_contrato_termo_aditivo_download.fn.__name__
)
