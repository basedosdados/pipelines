"""
Flows para br_ms_sinan — Prefect 3.

Migrado por completo pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote. Lógica específica do
dataset mora em `tasks.py`, constantes em `constants.py` — aqui só a fiação
(`CheckThenExtractLoadPipeline` + `@flow`).

O antigo `_run_sinan` monolítico segue existindo em
`pipelines/crawler/datasus/flows.py` (não removido daqui, já que outras
funções desse módulo seguem em uso por outros crawlers DATASUS).
"""

from pipelines.datasets.br_ms_sinan.constants import (
    DATASET_ID,
    MICRODADOS_DENGUE_TABLE_ID,
)
from pipelines.datasets.br_ms_sinan.tasks import (
    extract_load_data,
    get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import (
    CheckThenExtractLoadPipeline,
    Etapa,
    deploy_tags,
)

_microdados_dengue_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=MICRODADOS_DENGUE_TABLE_ID,
    get_latest_update=get_latest_update,
    extract_load_data=extract_load_data,
)


@flow(
    name=_microdados_dengue_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_ms_sinan_microdados_dengue_check_update() -> None:
    _microdados_dengue_pipeline.run_check_update()


br_ms_sinan_microdados_dengue_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, MICRODADOS_DENGUE_TABLE_ID
)
# Sem schedule no P0 — manter sem cron no P3 (`deploy_schedules` fica `None`,
# mesmo comportamento de antes).


@flow(
    name=_microdados_dengue_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_ms_sinan_microdados_dengue_download(download_params: dict) -> None:
    _microdados_dengue_pipeline.run_extract_and_load(download_params)


br_ms_sinan_microdados_dengue_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, MICRODADOS_DENGUE_TABLE_ID
)
_microdados_dengue_pipeline.extract_load_deployment = (
    br_ms_sinan_microdados_dengue_download.fn.__name__
)
