"""
Flows para br_inep_enem — Prefect 3.

Migrado por completo pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote, uma dupla de flows por
tabela. Lógica específica do dataset mora em `tasks.py`/`utils.py`, constantes
em `constants.py` — aqui só a fiação (`CheckThenExtractLoadPipeline` + `@flow`).

`questionario_socioeconomico_<ano>` fica fora: não tem flow agendado, de
propósito — cada edição é uma tabela nova, carregada à mão depois que ela
existe no backend (ver README.md).

O antigo `run_inep_enem` monolítico (poll/commit + loop de edições + run_dbt
manuais) não existe mais: `check_update_and_dispatch`/`dispatch_build_and_promote`
(em `pipelines.utils.stage_dispatch`) e o deployment genérico `build_and_promote`
cobrem o mesmo papel.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_inep_enem.constants import (
    DATASET_ID,
    PARTICIPANTES_TABLE_ID,
    RESULTADOS_TABLE_ID,
)
from pipelines.datasets.br_inep_enem.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# ──────────────────────────────────────────────────────────────────────────────
# participantes
# check_update: br_inep_enem__participantes
# extract_and_load: br_inep_enem__participantes
# ──────────────────────────────────────────────────────────────────────────────

_participantes_pipeline = make_pipeline(PARTICIPANTES_TABLE_ID)


@flow(name=_participantes_pipeline.check_update_flow_name, log_prints=True)
def br_inep_enem_participantes_check_update() -> None:
    _participantes_pipeline.run_check_update()


br_inep_enem_participantes_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, PARTICIPANTES_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_inep_enem_participantes_check_update.deploy_schedules = [
    Cron("13 9 12,13,14 * *", timezone="America/Sao_Paulo")
]


@flow(name=_participantes_pipeline.extract_and_load_flow_name, log_prints=True)
def br_inep_enem_participantes_download(download_params: dict) -> None:
    _participantes_pipeline.run_extract_and_load(download_params)


br_inep_enem_participantes_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, PARTICIPANTES_TABLE_ID
)
_participantes_pipeline.extract_load_deployment = (
    br_inep_enem_participantes_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# resultados
# check_update: br_inep_enem__resultados
# extract_and_load: br_inep_enem__resultados
# ──────────────────────────────────────────────────────────────────────────────

_resultados_pipeline = make_pipeline(RESULTADOS_TABLE_ID)


@flow(name=_resultados_pipeline.check_update_flow_name, log_prints=True)
def br_inep_enem_resultados_check_update() -> None:
    _resultados_pipeline.run_check_update()


br_inep_enem_resultados_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, RESULTADOS_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_inep_enem_resultados_check_update.deploy_schedules = [
    Cron("43 9 12,13,14 * *", timezone="America/Sao_Paulo")
]


@flow(name=_resultados_pipeline.extract_and_load_flow_name, log_prints=True)
def br_inep_enem_resultados_download(download_params: dict) -> None:
    _resultados_pipeline.run_extract_and_load(download_params)


br_inep_enem_resultados_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, RESULTADOS_TABLE_ID
)
_resultados_pipeline.extract_load_deployment = (
    br_inep_enem_resultados_download.fn.__name__
)
# `resultados` é o arquivo grande do conjunto (1,7 GB extraído, CSV lido em
# blocos) — mesmo `memory_limit` do flow monolítico antigo.
br_inep_enem_resultados_download.job_variables = {
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
