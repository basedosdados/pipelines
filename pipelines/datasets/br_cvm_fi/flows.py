"""
Flows para br_cvm_fi — Prefect 3.

Migrado por completo pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote, uma dupla de flows por
tabela. Lógica específica do dataset mora em `tasks.py`, constantes em
`constants.py` — aqui só a fiação (`CheckThenExtractLoadPipeline` + `@flow`).

O antigo `_cvm_fi_flow`/`_run_cvm_fi` monolítico segue existindo em
`pipelines/crawler/cvm/flows.py` (e `tasks.py`/`utils.py` no mesmo pacote),
de onde a lógica de scraping/parsing é reaproveitada por `tasks.py` deste
dataset — não removido daqui.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_cvm_fi.constants import (
    DATASET_ID,
    DOCUMENTOS_BALANCETE_TABLE_ID,
    DOCUMENTOS_CARTEIRAS_FUNDOS_INVESTIMENTO_TABLE_ID,
    DOCUMENTOS_EXTRATOS_INFORMACOES_TABLE_ID,
    DOCUMENTOS_INFORMACAO_CADASTRAL_TABLE_ID,
    DOCUMENTOS_INFORME_DIARIO_TABLE_ID,
    DOCUMENTOS_PERFIL_MENSAL_TABLE_ID,
)
from pipelines.datasets.br_cvm_fi.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# ──────────────────────────────────────────────────────────────────────────────
# documentos_informe_diario
# check_update: br_cvm_fi__documentos_informe_diario
# extract_and_load: br_cvm_fi__documentos_informe_diario
# ──────────────────────────────────────────────────────────────────────────────

_documentos_informe_diario_pipeline = make_pipeline(
    DOCUMENTOS_INFORME_DIARIO_TABLE_ID
)


@flow(
    name=_documentos_informe_diario_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cvm_fi_documentos_informe_diario_check_update() -> None:
    _documentos_informe_diario_pipeline.run_check_update()


br_cvm_fi_documentos_informe_diario_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, DOCUMENTOS_INFORME_DIARIO_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main) — tabelas escalonadas de dez em
# dez minutos a partir das 17h, pra não disputarem slot no BigQuery juntas.
br_cvm_fi_documentos_informe_diario_check_update.deploy_schedules = [
    Cron("0 17 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_documentos_informe_diario_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cvm_fi_documentos_informe_diario_download(
    download_params: dict,
) -> None:
    _documentos_informe_diario_pipeline.run_extract_and_load(download_params)


br_cvm_fi_documentos_informe_diario_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, DOCUMENTOS_INFORME_DIARIO_TABLE_ID
)
_documentos_informe_diario_pipeline.extract_load_deployment = (
    br_cvm_fi_documentos_informe_diario_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# documentos_carteiras_fundos_investimento
# check_update: br_cvm_fi__documentos_carteiras_fundos_investimento
# extract_and_load: br_cvm_fi__documentos_carteiras_fundos_investimento
# ──────────────────────────────────────────────────────────────────────────────

_documentos_carteiras_fundos_investimento_pipeline = make_pipeline(
    DOCUMENTOS_CARTEIRAS_FUNDOS_INVESTIMENTO_TABLE_ID
)


@flow(
    name=_documentos_carteiras_fundos_investimento_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cvm_fi_documentos_carteiras_fundos_investimento_check_update() -> None:
    _documentos_carteiras_fundos_investimento_pipeline.run_check_update()


br_cvm_fi_documentos_carteiras_fundos_investimento_check_update.deploy_tags = (
    deploy_tags(
        DATASET_ID,
        Etapa.CHECK_UPDATE,
        DOCUMENTOS_CARTEIRAS_FUNDOS_INVESTIMENTO_TABLE_ID,
    )
)
# Mesmo cron do flow monolítico antigo (main).
br_cvm_fi_documentos_carteiras_fundos_investimento_check_update.deploy_schedules = [
    Cron("10 17 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_documentos_carteiras_fundos_investimento_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cvm_fi_documentos_carteiras_fundos_investimento_download(
    download_params: dict,
) -> None:
    _documentos_carteiras_fundos_investimento_pipeline.run_extract_and_load(
        download_params
    )


br_cvm_fi_documentos_carteiras_fundos_investimento_download.deploy_tags = (
    deploy_tags(
        DATASET_ID,
        Etapa.EXTRACT_AND_LOAD,
        DOCUMENTOS_CARTEIRAS_FUNDOS_INVESTIMENTO_TABLE_ID,
    )
)
_documentos_carteiras_fundos_investimento_pipeline.extract_load_deployment = (
    br_cvm_fi_documentos_carteiras_fundos_investimento_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# documentos_extratos_informacoes
# check_update: br_cvm_fi__documentos_extratos_informacoes
# extract_and_load: br_cvm_fi__documentos_extratos_informacoes
# ──────────────────────────────────────────────────────────────────────────────

_documentos_extratos_informacoes_pipeline = make_pipeline(
    DOCUMENTOS_EXTRATOS_INFORMACOES_TABLE_ID
)


@flow(
    name=_documentos_extratos_informacoes_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cvm_fi_documentos_extratos_informacoes_check_update() -> None:
    _documentos_extratos_informacoes_pipeline.run_check_update()


br_cvm_fi_documentos_extratos_informacoes_check_update.deploy_tags = (
    deploy_tags(
        DATASET_ID,
        Etapa.CHECK_UPDATE,
        DOCUMENTOS_EXTRATOS_INFORMACOES_TABLE_ID,
    )
)
# Mesmo cron do flow monolítico antigo (main).
br_cvm_fi_documentos_extratos_informacoes_check_update.deploy_schedules = [
    Cron("20 17 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_documentos_extratos_informacoes_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cvm_fi_documentos_extratos_informacoes_download(
    download_params: dict,
) -> None:
    _documentos_extratos_informacoes_pipeline.run_extract_and_load(
        download_params
    )


br_cvm_fi_documentos_extratos_informacoes_download.deploy_tags = deploy_tags(
    DATASET_ID,
    Etapa.EXTRACT_AND_LOAD,
    DOCUMENTOS_EXTRATOS_INFORMACOES_TABLE_ID,
)
_documentos_extratos_informacoes_pipeline.extract_load_deployment = (
    br_cvm_fi_documentos_extratos_informacoes_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# documentos_balancete
# check_update: br_cvm_fi__documentos_balancete
# extract_and_load: br_cvm_fi__documentos_balancete
# ──────────────────────────────────────────────────────────────────────────────

_documentos_balancete_pipeline = make_pipeline(DOCUMENTOS_BALANCETE_TABLE_ID)


@flow(
    name=_documentos_balancete_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cvm_fi_documentos_balancete_check_update() -> None:
    _documentos_balancete_pipeline.run_check_update()


br_cvm_fi_documentos_balancete_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, DOCUMENTOS_BALANCETE_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_cvm_fi_documentos_balancete_check_update.deploy_schedules = [
    Cron("30 17 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_documentos_balancete_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cvm_fi_documentos_balancete_download(download_params: dict) -> None:
    _documentos_balancete_pipeline.run_extract_and_load(download_params)


br_cvm_fi_documentos_balancete_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, DOCUMENTOS_BALANCETE_TABLE_ID
)
_documentos_balancete_pipeline.extract_load_deployment = (
    br_cvm_fi_documentos_balancete_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# documentos_informacao_cadastral
# check_update: br_cvm_fi__documentos_informacao_cadastral
# extract_and_load: br_cvm_fi__documentos_informacao_cadastral
# ──────────────────────────────────────────────────────────────────────────────

_documentos_informacao_cadastral_pipeline = make_pipeline(
    DOCUMENTOS_INFORMACAO_CADASTRAL_TABLE_ID
)


@flow(
    name=_documentos_informacao_cadastral_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cvm_fi_documentos_informacao_cadastral_check_update() -> None:
    _documentos_informacao_cadastral_pipeline.run_check_update()


br_cvm_fi_documentos_informacao_cadastral_check_update.deploy_tags = (
    deploy_tags(
        DATASET_ID,
        Etapa.CHECK_UPDATE,
        DOCUMENTOS_INFORMACAO_CADASTRAL_TABLE_ID,
    )
)
# Mesmo cron do flow monolítico antigo (main).
br_cvm_fi_documentos_informacao_cadastral_check_update.deploy_schedules = [
    Cron("40 17 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_documentos_informacao_cadastral_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cvm_fi_documentos_informacao_cadastral_download(
    download_params: dict,
) -> None:
    _documentos_informacao_cadastral_pipeline.run_extract_and_load(
        download_params
    )


br_cvm_fi_documentos_informacao_cadastral_download.deploy_tags = deploy_tags(
    DATASET_ID,
    Etapa.EXTRACT_AND_LOAD,
    DOCUMENTOS_INFORMACAO_CADASTRAL_TABLE_ID,
)
_documentos_informacao_cadastral_pipeline.extract_load_deployment = (
    br_cvm_fi_documentos_informacao_cadastral_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# documentos_perfil_mensal
# check_update: br_cvm_fi__documentos_perfil_mensal
# extract_and_load: br_cvm_fi__documentos_perfil_mensal
# ──────────────────────────────────────────────────────────────────────────────

_documentos_perfil_mensal_pipeline = make_pipeline(
    DOCUMENTOS_PERFIL_MENSAL_TABLE_ID
)


@flow(
    name=_documentos_perfil_mensal_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cvm_fi_documentos_perfil_mensal_check_update() -> None:
    _documentos_perfil_mensal_pipeline.run_check_update()


br_cvm_fi_documentos_perfil_mensal_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, DOCUMENTOS_PERFIL_MENSAL_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_cvm_fi_documentos_perfil_mensal_check_update.deploy_schedules = [
    Cron("50 17 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_documentos_perfil_mensal_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cvm_fi_documentos_perfil_mensal_download(
    download_params: dict,
) -> None:
    _documentos_perfil_mensal_pipeline.run_extract_and_load(download_params)


br_cvm_fi_documentos_perfil_mensal_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, DOCUMENTOS_PERFIL_MENSAL_TABLE_ID
)
_documentos_perfil_mensal_pipeline.extract_load_deployment = (
    br_cvm_fi_documentos_perfil_mensal_download.fn.__name__
)
