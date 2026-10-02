"""
Flows para br_inmet_bdmep — Prefect 3.

Migrado por completo pro pipeline orientado a eventos:
check_update -> extract_and_load -> build_and_promote. Lógica específica do dataset mora em
`tasks.py`, constantes em `constants.py` — aqui só a fiação
(`CheckThenExtractLoadPipeline` + `@flow`).

O antigo flow monolítico (`br_inmet_bdmep__microdados`, cron às 22h de
seg-sex) foi removido deste arquivo.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_inmet_bdmep.constants import (
    DATASET_ID,
    MICRODADOS_TABLE_ID,
)
from pipelines.datasets.br_inmet_bdmep.tasks import (
    microdados_download,
    microdados_get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import (
    CheckThenExtractLoadPipeline,
    Etapa,
    deploy_tags,
)

_microdados_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=MICRODADOS_TABLE_ID,
    get_latest_update=microdados_get_latest_update,
    extract_load_data=microdados_download,
    # Mesma granularidade do flow antigo (comparava coverage com
    # date_format="%Y-%m").
    date_format="%Y-%m",
)


@flow(name=_microdados_pipeline.check_update_flow_name, log_prints=True)
def br_inmet_bdmep_microdados_check_update() -> None:
    _microdados_pipeline.run_check_update()


br_inmet_bdmep_microdados_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, MICRODADOS_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_inmet_bdmep_microdados_check_update.deploy_schedules = [
    Cron("0 22 * * 1-5", timezone="America/Sao_Paulo")
]


@flow(name=_microdados_pipeline.extract_and_load_flow_name, log_prints=True)
def br_inmet_bdmep_microdados_download(download_params: dict) -> None:
    _microdados_pipeline.run_extract_and_load(download_params)


br_inmet_bdmep_microdados_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, MICRODADOS_TABLE_ID
)
_microdados_pipeline.extract_load_deployment = (
    br_inmet_bdmep_microdados_download.fn.__name__
)
