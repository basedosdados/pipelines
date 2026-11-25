"""
Flows para br_ibge_ppm — Prefect 3.

Migrado por completo pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote, uma dupla de flows por
tabela. Lógica específica do dataset mora em `tasks.py`/`utils.py`, constantes
em `constants.py` — aqui só a fiação (`CheckThenExtractLoadPipeline` + `@flow`).

O antigo `run_ibge_ppm` monolítico (poll/commit + run_dbt + materialização
manuais) não existe mais: `check_update_and_dispatch`/`dispatch_build_and_promote`
(em `pipelines.utils.stage_dispatch`) e o deployment genérico `build_and_promote`
cobrem o mesmo papel.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_ibge_ppm.constants import (
    DATASET_ID,
    EFETIVO_REBANHOS_TABLE_ID,
    PRODUCAO_AQUICULTURA_TABLE_ID,
    PRODUCAO_ORIGEM_ANIMAL_TABLE_ID,
    PRODUCAO_PECUARIA_TABLE_ID,
)
from pipelines.datasets.br_ibge_ppm.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# ──────────────────────────────────────────────────────────────────────────────
# efetivo_rebanhos
# check_update: br_ibge_ppm__efetivo_rebanhos
# extract_and_load: br_ibge_ppm__efetivo_rebanhos
# ──────────────────────────────────────────────────────────────────────────────

_efetivo_rebanhos_pipeline = make_pipeline(EFETIVO_REBANHOS_TABLE_ID)


@flow(name=_efetivo_rebanhos_pipeline.check_update_flow_name, log_prints=True)
def br_ibge_ppm_efetivo_rebanhos_check_update() -> None:
    _efetivo_rebanhos_pipeline.run_check_update()


br_ibge_ppm_efetivo_rebanhos_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, EFETIVO_REBANHOS_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_ibge_ppm_efetivo_rebanhos_check_update.deploy_schedules = [
    Cron("4 14 15-30 9 *", timezone="America/Sao_Paulo"),
    Cron("4 14 * 10 *", timezone="America/Sao_Paulo"),
]


@flow(
    name=_efetivo_rebanhos_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_ibge_ppm_efetivo_rebanhos_download(download_params: dict) -> None:
    _efetivo_rebanhos_pipeline.run_extract_and_load(download_params)


br_ibge_ppm_efetivo_rebanhos_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, EFETIVO_REBANHOS_TABLE_ID
)
_efetivo_rebanhos_pipeline.extract_load_deployment = (
    br_ibge_ppm_efetivo_rebanhos_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# producao_origem_animal
# check_update: br_ibge_ppm__producao_origem_animal
# extract_and_load: br_ibge_ppm__producao_origem_animal
# ──────────────────────────────────────────────────────────────────────────────

_producao_origem_animal_pipeline = make_pipeline(
    PRODUCAO_ORIGEM_ANIMAL_TABLE_ID
)


@flow(
    name=_producao_origem_animal_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_ibge_ppm_producao_origem_animal_check_update() -> None:
    _producao_origem_animal_pipeline.run_check_update()


br_ibge_ppm_producao_origem_animal_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, PRODUCAO_ORIGEM_ANIMAL_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_ibge_ppm_producao_origem_animal_check_update.deploy_schedules = [
    Cron("9 14 15-30 9 *", timezone="America/Sao_Paulo"),
    Cron("9 14 * 10 *", timezone="America/Sao_Paulo"),
]


@flow(
    name=_producao_origem_animal_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_ibge_ppm_producao_origem_animal_download(
    download_params: dict,
) -> None:
    _producao_origem_animal_pipeline.run_extract_and_load(download_params)


br_ibge_ppm_producao_origem_animal_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, PRODUCAO_ORIGEM_ANIMAL_TABLE_ID
)
_producao_origem_animal_pipeline.extract_load_deployment = (
    br_ibge_ppm_producao_origem_animal_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# producao_aquicultura
# check_update: br_ibge_ppm__producao_aquicultura
# extract_and_load: br_ibge_ppm__producao_aquicultura
# ──────────────────────────────────────────────────────────────────────────────

_producao_aquicultura_pipeline = make_pipeline(PRODUCAO_AQUICULTURA_TABLE_ID)


@flow(
    name=_producao_aquicultura_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_ibge_ppm_producao_aquicultura_check_update() -> None:
    _producao_aquicultura_pipeline.run_check_update()


br_ibge_ppm_producao_aquicultura_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, PRODUCAO_AQUICULTURA_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_ibge_ppm_producao_aquicultura_check_update.deploy_schedules = [
    Cron("14 14 15-30 9 *", timezone="America/Sao_Paulo"),
    Cron("14 14 * 10 *", timezone="America/Sao_Paulo"),
]


@flow(
    name=_producao_aquicultura_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_ibge_ppm_producao_aquicultura_download(download_params: dict) -> None:
    _producao_aquicultura_pipeline.run_extract_and_load(download_params)


br_ibge_ppm_producao_aquicultura_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, PRODUCAO_AQUICULTURA_TABLE_ID
)
_producao_aquicultura_pipeline.extract_load_deployment = (
    br_ibge_ppm_producao_aquicultura_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# producao_pecuaria
# check_update: br_ibge_ppm__producao_pecuaria
# extract_and_load: br_ibge_ppm__producao_pecuaria
# ──────────────────────────────────────────────────────────────────────────────

_producao_pecuaria_pipeline = make_pipeline(PRODUCAO_PECUARIA_TABLE_ID)


@flow(
    name=_producao_pecuaria_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_ibge_ppm_producao_pecuaria_check_update() -> None:
    _producao_pecuaria_pipeline.run_check_update()


br_ibge_ppm_producao_pecuaria_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, PRODUCAO_PECUARIA_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_ibge_ppm_producao_pecuaria_check_update.deploy_schedules = [
    Cron("19 14 15-30 9 *", timezone="America/Sao_Paulo"),
    Cron("19 14 * 10 *", timezone="America/Sao_Paulo"),
]


@flow(
    name=_producao_pecuaria_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_ibge_ppm_producao_pecuaria_download(download_params: dict) -> None:
    _producao_pecuaria_pipeline.run_extract_and_load(download_params)


br_ibge_ppm_producao_pecuaria_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, PRODUCAO_PECUARIA_TABLE_ID
)
_producao_pecuaria_pipeline.extract_load_deployment = (
    br_ibge_ppm_producao_pecuaria_download.fn.__name__
)
