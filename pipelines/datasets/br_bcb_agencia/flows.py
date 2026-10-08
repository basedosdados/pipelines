"""
Flows para br_bcb_agencia — Prefect 3.

Migrado por completo pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote. Lógica específica do
dataset mora em `tasks.py`, constantes em `constants.py` — aqui só a fiação
(`CheckThenExtractLoadPipeline` + `@flow`).
"""

from prefect.schedules import Cron

from pipelines.datasets.br_bcb_agencia.constants import (
    AGENCIA_TABLE_ID,
    DATASET_ID,
)
from pipelines.datasets.br_bcb_agencia.tasks import (
    extract_load_data,
    get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import (
    CheckThenExtractLoadPipeline,
    Etapa,
    deploy_tags,
)

_agencia_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=AGENCIA_TABLE_ID,
    get_latest_update=get_latest_update,
    extract_load_data=extract_load_data,
    # Mesma granularidade do flow antigo: dado é mensal, sem dia.
    date_format="%Y-%m",
)


@flow(name=_agencia_pipeline.check_update_flow_name, log_prints=True)
def br_bcb_agencia_agencia_check_update() -> None:
    _agencia_pipeline.run_check_update()


br_bcb_agencia_agencia_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, AGENCIA_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_bcb_agencia_agencia_check_update.deploy_schedules = [
    Cron("0 22 25-31 * *", timezone="America/Sao_Paulo")
]


@flow(name=_agencia_pipeline.extract_and_load_flow_name, log_prints=True)
def br_bcb_agencia_agencia_download(download_params: dict) -> None:
    _agencia_pipeline.run_extract_and_load(download_params)


br_bcb_agencia_agencia_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, AGENCIA_TABLE_ID
)
_agencia_pipeline.extract_load_deployment = (
    br_bcb_agencia_agencia_download.fn.__name__
)
