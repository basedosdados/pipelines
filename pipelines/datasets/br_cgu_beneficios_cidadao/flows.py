"""
Flows para br_cgu_beneficios_cidadao — Prefect 3.

Migrado por completo pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote, uma dupla de flows
por tabela (`novo_bolsa_familia`, `garantia_safra`, `bpc`). Lógica
específica do dataset mora em `tasks.py`, constantes em `constants.py` —
aqui só a fiação (`CheckThenExtractLoadPipeline` + `@flow`).

A fonte não expõe data de última atualização fora do próprio arquivo —
`get_latest_update` baixa o ZIP do mês candidato pra descobrir se há mês
novo (ver banner de `tasks.py`); `extract_and_load` baixa de novo, de forma
independente.

O flow monolítico antigo (`_run_cgu_beneficios_cidadao`, em
`pipelines/crawler/cgu/flows.py`) segue existindo — ainda usado pelos
outros datasets `br_cgu_*` (cartão de pagamento, servidores, licitação e
contrato), não migrados aqui.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_cgu_beneficios_cidadao.constants import (
    BPC_TABLE_ID,
    DATASET_ID,
    GARANTIA_SAFRA_TABLE_ID,
    NOVO_BOLSA_FAMILIA_TABLE_ID,
)
from pipelines.datasets.br_cgu_beneficios_cidadao.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# ──────────────────────────────────────────────────────────────────────────────
# novo_bolsa_familia
# check_update: br_cgu_beneficios_cidadao__novo_bolsa_familia
# extract_and_load: br_cgu_beneficios_cidadao__novo_bolsa_familia
# ──────────────────────────────────────────────────────────────────────────────

_novo_bolsa_familia_pipeline = make_pipeline(NOVO_BOLSA_FAMILIA_TABLE_ID)


@flow(
    name=_novo_bolsa_familia_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cgu_beneficios_cidadao_novo_bolsa_familia_check_update() -> None:
    _novo_bolsa_familia_pipeline.run_check_update()


br_cgu_beneficios_cidadao_novo_bolsa_familia_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, NOVO_BOLSA_FAMILIA_TABLE_ID)
)
# Mesmo cron do flow monolítico antigo (main).
br_cgu_beneficios_cidadao_novo_bolsa_familia_check_update.deploy_schedules = [
    Cron("0 19 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_novo_bolsa_familia_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cgu_beneficios_cidadao_novo_bolsa_familia_download(
    download_params: dict,
) -> None:
    _novo_bolsa_familia_pipeline.run_extract_and_load(download_params)


br_cgu_beneficios_cidadao_novo_bolsa_familia_download.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, NOVO_BOLSA_FAMILIA_TABLE_ID
    )
)
_novo_bolsa_familia_pipeline.extract_load_deployment = (
    br_cgu_beneficios_cidadao_novo_bolsa_familia_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# garantia_safra
# check_update: br_cgu_beneficios_cidadao__garantia_safra
# extract_and_load: br_cgu_beneficios_cidadao__garantia_safra
# ──────────────────────────────────────────────────────────────────────────────

_garantia_safra_pipeline = make_pipeline(GARANTIA_SAFRA_TABLE_ID)


@flow(
    name=_garantia_safra_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cgu_beneficios_cidadao_garantia_safra_check_update() -> None:
    _garantia_safra_pipeline.run_check_update()


br_cgu_beneficios_cidadao_garantia_safra_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, GARANTIA_SAFRA_TABLE_ID)
)
# Mesmo cron do flow monolítico antigo (main).
br_cgu_beneficios_cidadao_garantia_safra_check_update.deploy_schedules = [
    Cron("15 19 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_garantia_safra_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cgu_beneficios_cidadao_garantia_safra_download(
    download_params: dict,
) -> None:
    _garantia_safra_pipeline.run_extract_and_load(download_params)


br_cgu_beneficios_cidadao_garantia_safra_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, GARANTIA_SAFRA_TABLE_ID
)
_garantia_safra_pipeline.extract_load_deployment = (
    br_cgu_beneficios_cidadao_garantia_safra_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# bpc
# check_update: br_cgu_beneficios_cidadao__bpc
# extract_and_load: br_cgu_beneficios_cidadao__bpc
# ──────────────────────────────────────────────────────────────────────────────

_bpc_pipeline = make_pipeline(BPC_TABLE_ID)


@flow(name=_bpc_pipeline.check_update_flow_name, log_prints=True)
def br_cgu_beneficios_cidadao_bpc_check_update() -> None:
    _bpc_pipeline.run_check_update()


br_cgu_beneficios_cidadao_bpc_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, BPC_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_cgu_beneficios_cidadao_bpc_check_update.deploy_schedules = [
    Cron("30 19 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_bpc_pipeline.extract_and_load_flow_name, log_prints=True)
def br_cgu_beneficios_cidadao_bpc_download(download_params: dict) -> None:
    _bpc_pipeline.run_extract_and_load(download_params)


br_cgu_beneficios_cidadao_bpc_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, BPC_TABLE_ID
)
_bpc_pipeline.extract_load_deployment = (
    br_cgu_beneficios_cidadao_bpc_download.fn.__name__
)
