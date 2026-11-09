"""
Flows para br_bcb_taxa_cambio — Prefect 3.

Migrado por completo pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote. Lógica específica do
dataset mora em `tasks.py`, constantes em `constants.py` — aqui só a fiação
(`CheckThenExtractLoadPipeline` + `@flow`).
"""

from prefect.schedules import Cron

from pipelines.datasets.br_bcb_taxa_cambio.constants import (
    DATASET_ID,
    TAXA_CAMBIO_TABLE_ID,
)
from pipelines.datasets.br_bcb_taxa_cambio.tasks import (
    extract_load_data,
    get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import (
    CheckThenExtractLoadPipeline,
    Etapa,
    deploy_tags,
)

_taxa_cambio_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=TAXA_CAMBIO_TABLE_ID,
    get_latest_update=get_latest_update,
    extract_load_data=extract_load_data,
)


@flow(name=_taxa_cambio_pipeline.check_update_flow_name, log_prints=True)
def br_bcb_taxa_cambio_taxa_cambio_check_update() -> None:
    _taxa_cambio_pipeline.run_check_update()


br_bcb_taxa_cambio_taxa_cambio_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, TAXA_CAMBIO_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_bcb_taxa_cambio_taxa_cambio_check_update.deploy_schedules = [
    Cron("40 8 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_taxa_cambio_pipeline.extract_and_load_flow_name, log_prints=True)
def br_bcb_taxa_cambio_taxa_cambio_download(download_params: dict) -> None:
    _taxa_cambio_pipeline.run_extract_and_load(download_params)


br_bcb_taxa_cambio_taxa_cambio_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, TAXA_CAMBIO_TABLE_ID
)
_taxa_cambio_pipeline.extract_load_deployment = (
    br_bcb_taxa_cambio_taxa_cambio_download.fn.__name__
)
