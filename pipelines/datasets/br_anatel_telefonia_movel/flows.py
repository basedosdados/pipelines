"""
Flows para br_anatel_telefonia_movel — Prefect 3.

Migrado por completo pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote, uma dupla de flows por
tabela. Lógica específica do dataset mora em `tasks.py`, constantes em
`constants.py` — aqui só a fiação (`CheckThenExtractLoadPipeline` + `@flow`).
Ver README.md deste diretório pro histórico completo da fonte (revisão
retroativa, bug do cast de `acessos` em 2025, poll por geografia) e a nota em
`tasks.py` sobre o trade-off do commit não-deferido desta arquitetura.

As 4 tabelas mantêm os crons do flow monolítico antigo (`_anatel_tm_flow`,
este módulo antes da migração), um dia da semana cada, e o mesmo
`job_variables` (memória) — agora aplicado às duas etapas, já que tanto
check_update quanto extract_and_load baixam/descompactam o ZIP e leem os CSVs
via pandas (ambos pesados, diferente do flow antigo que fazia tudo de uma vez
só).

O flow monolítico antigo (`_run_anatel_telefonia_movel`, que vivia em
`pipelines/crawler/anatel/telefonia_movel/flows.py`) não tinha mais
consumidor após esta migração e foi removido; as tasks daquele módulo foram
incorporadas a `tasks.py` deste dataset.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_anatel_telefonia_movel.constants import (
    DATASET_ID,
    DENSIDADE_BRASIL_TABLE_ID,
    DENSIDADE_MUNICIPIO_TABLE_ID,
    DENSIDADE_UF_TABLE_ID,
    MICRODADOS_TABLE_ID,
)
from pipelines.datasets.br_anatel_telefonia_movel.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# Mesmo limite de memória do flow monolítico antigo — unzip + leitura dos
# CSVs via pandas é pesado nas duas etapas agora.
_JOB_VARIABLES = {"memory_limit": "8Gi", "memory_request": "2Gi"}

# ──────────────────────────────────────────────────────────────────────────────
# microdados
# check_update: br_anatel_telefonia_movel__microdados
# extract_and_load: br_anatel_telefonia_movel__microdados
# ──────────────────────────────────────────────────────────────────────────────

_microdados_pipeline = make_pipeline(MICRODADOS_TABLE_ID)


@flow(name=_microdados_pipeline.check_update_flow_name, log_prints=True)
def br_anatel_telefonia_movel_microdados_check_update() -> None:
    _microdados_pipeline.run_check_update()


br_anatel_telefonia_movel_microdados_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, MICRODADOS_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (`_anatel_tm_flow(cron="30 16 * * 1")`).
br_anatel_telefonia_movel_microdados_check_update.deploy_schedules = [
    Cron("30 16 * * 1", timezone="America/Sao_Paulo")
]
br_anatel_telefonia_movel_microdados_check_update.job_variables = (
    _JOB_VARIABLES
)


@flow(name=_microdados_pipeline.extract_and_load_flow_name, log_prints=True)
def br_anatel_telefonia_movel_microdados_download(
    download_params: dict,
) -> None:
    _microdados_pipeline.run_extract_and_load(download_params)


br_anatel_telefonia_movel_microdados_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, MICRODADOS_TABLE_ID
)
br_anatel_telefonia_movel_microdados_download.job_variables = _JOB_VARIABLES
_microdados_pipeline.extract_load_deployment = (
    br_anatel_telefonia_movel_microdados_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# densidade_municipio
# check_update: br_anatel_telefonia_movel__densidade_municipio
# extract_and_load: br_anatel_telefonia_movel__densidade_municipio
# ──────────────────────────────────────────────────────────────────────────────

_densidade_municipio_pipeline = make_pipeline(DENSIDADE_MUNICIPIO_TABLE_ID)


@flow(
    name=_densidade_municipio_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_anatel_telefonia_movel_densidade_municipio_check_update() -> None:
    _densidade_municipio_pipeline.run_check_update()


br_anatel_telefonia_movel_densidade_municipio_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, DENSIDADE_MUNICIPIO_TABLE_ID)
)
# Mesmo cron do flow monolítico antigo (`_anatel_tm_flow(cron="30 16 * * 2")`).
br_anatel_telefonia_movel_densidade_municipio_check_update.deploy_schedules = [
    Cron("30 16 * * 2", timezone="America/Sao_Paulo")
]
br_anatel_telefonia_movel_densidade_municipio_check_update.job_variables = (
    _JOB_VARIABLES
)


@flow(
    name=_densidade_municipio_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_anatel_telefonia_movel_densidade_municipio_download(
    download_params: dict,
) -> None:
    _densidade_municipio_pipeline.run_extract_and_load(download_params)


br_anatel_telefonia_movel_densidade_municipio_download.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, DENSIDADE_MUNICIPIO_TABLE_ID
    )
)
br_anatel_telefonia_movel_densidade_municipio_download.job_variables = (
    _JOB_VARIABLES
)
_densidade_municipio_pipeline.extract_load_deployment = (
    br_anatel_telefonia_movel_densidade_municipio_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# densidade_uf
# check_update: br_anatel_telefonia_movel__densidade_uf
# extract_and_load: br_anatel_telefonia_movel__densidade_uf
# ──────────────────────────────────────────────────────────────────────────────

_densidade_uf_pipeline = make_pipeline(DENSIDADE_UF_TABLE_ID)


@flow(name=_densidade_uf_pipeline.check_update_flow_name, log_prints=True)
def br_anatel_telefonia_movel_densidade_uf_check_update() -> None:
    _densidade_uf_pipeline.run_check_update()


br_anatel_telefonia_movel_densidade_uf_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, DENSIDADE_UF_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (`_anatel_tm_flow(cron="30 16 * * 3")`).
br_anatel_telefonia_movel_densidade_uf_check_update.deploy_schedules = [
    Cron("30 16 * * 3", timezone="America/Sao_Paulo")
]
br_anatel_telefonia_movel_densidade_uf_check_update.job_variables = (
    _JOB_VARIABLES
)


@flow(
    name=_densidade_uf_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_anatel_telefonia_movel_densidade_uf_download(
    download_params: dict,
) -> None:
    _densidade_uf_pipeline.run_extract_and_load(download_params)


br_anatel_telefonia_movel_densidade_uf_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, DENSIDADE_UF_TABLE_ID
)
br_anatel_telefonia_movel_densidade_uf_download.job_variables = _JOB_VARIABLES
_densidade_uf_pipeline.extract_load_deployment = (
    br_anatel_telefonia_movel_densidade_uf_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# densidade_brasil
# check_update: br_anatel_telefonia_movel__densidade_brasil
# extract_and_load: br_anatel_telefonia_movel__densidade_brasil
# ──────────────────────────────────────────────────────────────────────────────

_densidade_brasil_pipeline = make_pipeline(DENSIDADE_BRASIL_TABLE_ID)


@flow(
    name=_densidade_brasil_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_anatel_telefonia_movel_densidade_brasil_check_update() -> None:
    _densidade_brasil_pipeline.run_check_update()


br_anatel_telefonia_movel_densidade_brasil_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, DENSIDADE_BRASIL_TABLE_ID)
)
# Mesmo cron do flow monolítico antigo (`_anatel_tm_flow(cron="30 16 * * 4")`).
br_anatel_telefonia_movel_densidade_brasil_check_update.deploy_schedules = [
    Cron("30 16 * * 4", timezone="America/Sao_Paulo")
]
br_anatel_telefonia_movel_densidade_brasil_check_update.job_variables = (
    _JOB_VARIABLES
)


@flow(
    name=_densidade_brasil_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_anatel_telefonia_movel_densidade_brasil_download(
    download_params: dict,
) -> None:
    _densidade_brasil_pipeline.run_extract_and_load(download_params)


br_anatel_telefonia_movel_densidade_brasil_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, DENSIDADE_BRASIL_TABLE_ID
)
br_anatel_telefonia_movel_densidade_brasil_download.job_variables = (
    _JOB_VARIABLES
)
_densidade_brasil_pipeline.extract_load_deployment = (
    br_anatel_telefonia_movel_densidade_brasil_download.fn.__name__
)
