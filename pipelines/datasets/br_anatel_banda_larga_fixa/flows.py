"""
Flows para br_anatel_banda_larga_fixa — Prefect 3.

Migrado por completo pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote, uma dupla de flows por
tabela. Lógica específica do dataset mora em `tasks.py`, constantes em
`constants.py` — aqui só a fiação (`CheckThenExtractLoadPipeline` + `@flow`).
Ver README.md deste diretório pro histórico completo da migração (causa raiz
do congelamento, revisão histórica do microdados, apagão das densidades).

As 3 tabelas de densidade (`densidade_brasil`, `densidade_uf`,
`densidade_municipio`) seguem SEM schedule (nenhum `deploy_schedules`
setado): a fonte apagou 2023-2025 nelas, e um run automático re-materializaria
do arquivo degradado e regrediria prod (ver README.md, seção Densidades). Só
rodam via trigger manual do `check_update` até a decisão da PRUV. Os crons
originais (desativados) seguem documentados em comentário em cada bloco,
prontos para reativar quando a fonte for restaurada.

`pipelines/crawler/anatel/banda_larga_fixa/flows.py` (`_run_anatel_banda_larga_fixa`,
o flow monolítico antigo) não é mais usado por este dataset — fica preservado
como histórico, sem outro consumidor no repositório.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_anatel_banda_larga_fixa.constants import (
    DATASET_ID,
    DENSIDADE_BRASIL_TABLE_ID,
    DENSIDADE_MUNICIPIO_TABLE_ID,
    DENSIDADE_UF_TABLE_ID,
    MICRODADOS_TABLE_ID,
)
from pipelines.datasets.br_anatel_banda_larga_fixa.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# ──────────────────────────────────────────────────────────────────────────────
# microdados
# check_update: br_anatel_banda_larga_fixa__microdados
# extract_and_load: br_anatel_banda_larga_fixa__microdados
# ──────────────────────────────────────────────────────────────────────────────

_microdados_pipeline = make_pipeline(MICRODADOS_TABLE_ID)


@flow(name=_microdados_pipeline.check_update_flow_name, log_prints=True)
def br_anatel_banda_larga_fixa_microdados_check_update() -> None:
    _microdados_pipeline.run_check_update()


br_anatel_banda_larga_fixa_microdados_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, MICRODADOS_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (`_anatel_blf_flow(cron="0 15 * * *")`
# em `pipelines/crawler/anatel/banda_larga_fixa` — único dos 4 com schedule
# ativo, por ser o único que não regride com a revisão histórica da fonte).
br_anatel_banda_larga_fixa_microdados_check_update.deploy_schedules = [
    Cron("0 15 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_microdados_pipeline.extract_and_load_flow_name, log_prints=True)
def br_anatel_banda_larga_fixa_microdados_download(
    download_params: dict,
) -> None:
    _microdados_pipeline.run_extract_and_load(download_params)


br_anatel_banda_larga_fixa_microdados_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, MICRODADOS_TABLE_ID
)
_microdados_pipeline.extract_load_deployment = (
    br_anatel_banda_larga_fixa_microdados_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# densidade_brasil
# check_update: br_anatel_banda_larga_fixa__densidade_brasil
# extract_and_load: br_anatel_banda_larga_fixa__densidade_brasil
#
# Sem schedule — ver docstring do módulo. Cron original (desativado):
# "0 17 * * *".
# ──────────────────────────────────────────────────────────────────────────────

_densidade_brasil_pipeline = make_pipeline(DENSIDADE_BRASIL_TABLE_ID)


@flow(
    name=_densidade_brasil_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_anatel_banda_larga_fixa_densidade_brasil_check_update() -> None:
    _densidade_brasil_pipeline.run_check_update()


br_anatel_banda_larga_fixa_densidade_brasil_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, DENSIDADE_BRASIL_TABLE_ID)
)


@flow(
    name=_densidade_brasil_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_anatel_banda_larga_fixa_densidade_brasil_download(
    download_params: dict,
) -> None:
    _densidade_brasil_pipeline.run_extract_and_load(download_params)


br_anatel_banda_larga_fixa_densidade_brasil_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, DENSIDADE_BRASIL_TABLE_ID
)
_densidade_brasil_pipeline.extract_load_deployment = (
    br_anatel_banda_larga_fixa_densidade_brasil_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# densidade_uf
# check_update: br_anatel_banda_larga_fixa__densidade_uf
# extract_and_load: br_anatel_banda_larga_fixa__densidade_uf
#
# Sem schedule — ver docstring do módulo. Cron original (desativado):
# "0 18 * * *".
# ──────────────────────────────────────────────────────────────────────────────

_densidade_uf_pipeline = make_pipeline(DENSIDADE_UF_TABLE_ID)


@flow(name=_densidade_uf_pipeline.check_update_flow_name, log_prints=True)
def br_anatel_banda_larga_fixa_densidade_uf_check_update() -> None:
    _densidade_uf_pipeline.run_check_update()


br_anatel_banda_larga_fixa_densidade_uf_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, DENSIDADE_UF_TABLE_ID
)


@flow(
    name=_densidade_uf_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_anatel_banda_larga_fixa_densidade_uf_download(
    download_params: dict,
) -> None:
    _densidade_uf_pipeline.run_extract_and_load(download_params)


br_anatel_banda_larga_fixa_densidade_uf_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, DENSIDADE_UF_TABLE_ID
)
_densidade_uf_pipeline.extract_load_deployment = (
    br_anatel_banda_larga_fixa_densidade_uf_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# densidade_municipio
# check_update: br_anatel_banda_larga_fixa__densidade_municipio
# extract_and_load: br_anatel_banda_larga_fixa__densidade_municipio
#
# Sem schedule — ver docstring do módulo. Cron original (desativado):
# "0 16 * * *".
# ──────────────────────────────────────────────────────────────────────────────

_densidade_municipio_pipeline = make_pipeline(DENSIDADE_MUNICIPIO_TABLE_ID)


@flow(
    name=_densidade_municipio_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_anatel_banda_larga_fixa_densidade_municipio_check_update() -> None:
    _densidade_municipio_pipeline.run_check_update()


br_anatel_banda_larga_fixa_densidade_municipio_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, DENSIDADE_MUNICIPIO_TABLE_ID)
)


@flow(
    name=_densidade_municipio_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_anatel_banda_larga_fixa_densidade_municipio_download(
    download_params: dict,
) -> None:
    _densidade_municipio_pipeline.run_extract_and_load(download_params)


br_anatel_banda_larga_fixa_densidade_municipio_download.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, DENSIDADE_MUNICIPIO_TABLE_ID
    )
)
_densidade_municipio_pipeline.extract_load_deployment = (
    br_anatel_banda_larga_fixa_densidade_municipio_download.fn.__name__
)
