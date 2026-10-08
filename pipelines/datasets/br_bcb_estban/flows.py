"""
Flows para br_bcb_estban — Prefect 3.

Migrado por completo pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote, uma dupla de flows por
tabela. Lógica específica do dataset mora em `tasks.py`, constantes em
`constants.py` — aqui só a fiação (`CheckThenExtractLoadPipeline` + `@flow`).

O antigo `_run_bcb_estban`/`_estban_flow` monolítico foi substituído por
completo por este módulo — as tasks/utils de `pipelines/crawler/bcb_estban/`
seguem existindo e são reaproveitadas aqui (`tasks.py`), sem reescrever a
lógica de negócio.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_bcb_estban.constants import (
    AGENCIA_TABLE_ID,
    DATASET_ID,
    MUNICIPIO_TABLE_ID,
)
from pipelines.datasets.br_bcb_estban.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# ──────────────────────────────────────────────────────────────────────────────
# agencia
# check_update: br_bcb_estban__agencia
# extract_and_load: br_bcb_estban__agencia
# ──────────────────────────────────────────────────────────────────────────────

_agencia_pipeline = make_pipeline(AGENCIA_TABLE_ID)


@flow(name=_agencia_pipeline.check_update_flow_name, log_prints=True)
def br_bcb_estban_agencia_check_update() -> None:
    _agencia_pipeline.run_check_update()


br_bcb_estban_agencia_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, AGENCIA_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (`br_bcb_estban__agencia`).
br_bcb_estban_agencia_check_update.deploy_schedules = [
    Cron("0 22 25-31 * *", timezone="America/Sao_Paulo")
]


@flow(name=_agencia_pipeline.extract_and_load_flow_name, log_prints=True)
def br_bcb_estban_agencia_download(download_params: dict) -> None:
    _agencia_pipeline.run_extract_and_load(download_params)


br_bcb_estban_agencia_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, AGENCIA_TABLE_ID
)
_agencia_pipeline.extract_load_deployment = (
    br_bcb_estban_agencia_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# municipio
# check_update: br_bcb_estban__municipio
# extract_and_load: br_bcb_estban__municipio
# ──────────────────────────────────────────────────────────────────────────────

_municipio_pipeline = make_pipeline(MUNICIPIO_TABLE_ID)


@flow(name=_municipio_pipeline.check_update_flow_name, log_prints=True)
def br_bcb_estban_municipio_check_update() -> None:
    _municipio_pipeline.run_check_update()


br_bcb_estban_municipio_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, MUNICIPIO_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (`br_bcb_estban__municipio`).
br_bcb_estban_municipio_check_update.deploy_schedules = [
    Cron("30 22 25-31 * *", timezone="America/Sao_Paulo")
]


@flow(name=_municipio_pipeline.extract_and_load_flow_name, log_prints=True)
def br_bcb_estban_municipio_download(download_params: dict) -> None:
    _municipio_pipeline.run_extract_and_load(download_params)


br_bcb_estban_municipio_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, MUNICIPIO_TABLE_ID
)
_municipio_pipeline.extract_load_deployment = (
    br_bcb_estban_municipio_download.fn.__name__
)
