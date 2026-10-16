"""
Flows para br_cgu_cartao_pagamento — Prefect 3.

Migrado por completo pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote, uma dupla de flows por
tabela. Lógica específica do dataset mora em `tasks.py`, constantes em
`constants.py` — aqui só a fiação (`CheckThenExtractLoadPipeline` + `@flow`).

As 3 tabelas baixam de forma independente (cada uma seu próprio ZIP do
Portal da Transparência) — sem dependência real de ordem entre elas no dbt.
O teste `custom_dictionary_coverage` de cada tabela referencia
`br_cgu_cartao_pagamento__dicionario` via `ref()`, mas essa tabela é estática
e mantida fora deste pipeline (ver `constants.py`), não uma das 3 tabelas
migradas aqui — por isso 3 pares check_update/extract_and_load
independentes, sem materialização em lote.

O antigo `_run_cgu_cartao_pagamento` monolítico segue existindo em
`pipelines/crawler/cgu/flows.py`, compartilhado com outros datasets br_cgu_*
ainda não migrados (`br_cgu_servidores_executivo_federal`,
`br_cgu_licitacao_contrato`) — não removido daqui.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_cgu_cartao_pagamento.constants import (
    DATASET_ID,
    MICRODADOS_COMPRAS_CENTRALIZADAS_TABLE_ID,
    MICRODADOS_DEFESA_CIVIL_TABLE_ID,
    MICRODADOS_GOVERNO_FEDERAL_TABLE_ID,
)
from pipelines.datasets.br_cgu_cartao_pagamento.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# ──────────────────────────────────────────────────────────────────────────────
# microdados_governo_federal
# check_update: br_cgu_cartao_pagamento__microdados_governo_federal
# extract_and_load: br_cgu_cartao_pagamento__microdados_governo_federal
# ──────────────────────────────────────────────────────────────────────────────

_microdados_governo_federal_pipeline = make_pipeline(
    MICRODADOS_GOVERNO_FEDERAL_TABLE_ID
)


@flow(
    name=_microdados_governo_federal_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cgu_cartao_pagamento_microdados_governo_federal_check_update() -> None:
    _microdados_governo_federal_pipeline.run_check_update()


br_cgu_cartao_pagamento_microdados_governo_federal_check_update.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.CHECK_UPDATE, MICRODADOS_GOVERNO_FEDERAL_TABLE_ID
    )
)
# Mesmo cron do flow monolítico antigo (main).
br_cgu_cartao_pagamento_microdados_governo_federal_check_update.deploy_schedules = [
    Cron("0 20 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_microdados_governo_federal_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cgu_cartao_pagamento_microdados_governo_federal_download(
    download_params: dict,
) -> None:
    _microdados_governo_federal_pipeline.run_extract_and_load(download_params)


br_cgu_cartao_pagamento_microdados_governo_federal_download.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, MICRODADOS_GOVERNO_FEDERAL_TABLE_ID
    )
)
_microdados_governo_federal_pipeline.extract_load_deployment = (
    br_cgu_cartao_pagamento_microdados_governo_federal_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# microdados_compras_centralizadas
# check_update: br_cgu_cartao_pagamento__microdados_compras_centralizadas
# extract_and_load: br_cgu_cartao_pagamento__microdados_compras_centralizadas
# ──────────────────────────────────────────────────────────────────────────────

_microdados_compras_centralizadas_pipeline = make_pipeline(
    MICRODADOS_COMPRAS_CENTRALIZADAS_TABLE_ID
)


@flow(
    name=_microdados_compras_centralizadas_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cgu_cartao_pagamento_microdados_compras_centralizadas_check_update() -> (
    None
):
    _microdados_compras_centralizadas_pipeline.run_check_update()


br_cgu_cartao_pagamento_microdados_compras_centralizadas_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, MICRODADOS_COMPRAS_CENTRALIZADAS_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_cgu_cartao_pagamento_microdados_compras_centralizadas_check_update.deploy_schedules = [
    Cron("0 21 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_microdados_compras_centralizadas_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cgu_cartao_pagamento_microdados_compras_centralizadas_download(
    download_params: dict,
) -> None:
    _microdados_compras_centralizadas_pipeline.run_extract_and_load(
        download_params
    )


br_cgu_cartao_pagamento_microdados_compras_centralizadas_download.deploy_tags = deploy_tags(
    DATASET_ID,
    Etapa.EXTRACT_AND_LOAD,
    MICRODADOS_COMPRAS_CENTRALIZADAS_TABLE_ID,
)
_microdados_compras_centralizadas_pipeline.extract_load_deployment = br_cgu_cartao_pagamento_microdados_compras_centralizadas_download.fn.__name__


# ──────────────────────────────────────────────────────────────────────────────
# microdados_defesa_civil
# check_update: br_cgu_cartao_pagamento__microdados_defesa_civil
# extract_and_load: br_cgu_cartao_pagamento__microdados_defesa_civil
# ──────────────────────────────────────────────────────────────────────────────

_microdados_defesa_civil_pipeline = make_pipeline(
    MICRODADOS_DEFESA_CIVIL_TABLE_ID
)


@flow(
    name=_microdados_defesa_civil_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cgu_cartao_pagamento_microdados_defesa_civil_check_update() -> None:
    _microdados_defesa_civil_pipeline.run_check_update()


br_cgu_cartao_pagamento_microdados_defesa_civil_check_update.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.CHECK_UPDATE, MICRODADOS_DEFESA_CIVIL_TABLE_ID
    )
)
# Mesmo cron do flow monolítico antigo (main).
br_cgu_cartao_pagamento_microdados_defesa_civil_check_update.deploy_schedules = [
    Cron("30 20 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_microdados_defesa_civil_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cgu_cartao_pagamento_microdados_defesa_civil_download(
    download_params: dict,
) -> None:
    _microdados_defesa_civil_pipeline.run_extract_and_load(download_params)


br_cgu_cartao_pagamento_microdados_defesa_civil_download.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, MICRODADOS_DEFESA_CIVIL_TABLE_ID
    )
)
_microdados_defesa_civil_pipeline.extract_load_deployment = (
    br_cgu_cartao_pagamento_microdados_defesa_civil_download.fn.__name__
)
