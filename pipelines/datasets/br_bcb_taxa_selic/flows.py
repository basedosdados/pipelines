"""
Flows para br_bcb_taxa_selic — Prefect 3.

Migrado pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote. Lógica específica do
dataset mora em `tasks.py`, constantes em `constants.py` — aqui só a fiação
(`CheckThenExtractLoadPipeline` + `@flow`).

A série da Selic (BCB SGS 11) não aceita filtro por data — toda chamada
devolve a série completa. Por isso `check_update` baixa o dado inteiro só
pra achar a data mais recente, e `extract_and_load` baixa de novo de forma
independente (custo duplicado aceito; a variante `check_and_download` que
evitaria isso foi cancelada).
"""

from prefect.schedules import Cron

from pipelines.datasets.br_bcb_taxa_selic.constants import DATASET_ID, TABLE_ID
from pipelines.datasets.br_bcb_taxa_selic.tasks import (
    extract_load_data,
    get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import (
    CheckThenExtractLoadPipeline,
    Etapa,
    deploy_tags,
)

_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=TABLE_ID,
    get_latest_update=get_latest_update,
    extract_load_data=extract_load_data,
)


@flow(name=_pipeline.check_update_flow_name, log_prints=True)
def check_update() -> None:
    _pipeline.run_check_update()


check_update.deploy_tags = deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE)
# Mesmo cron do flow monolítico antigo (main).
check_update.deploy_schedules = [
    Cron("0 8 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_pipeline.extract_and_load_flow_name, log_prints=True)
def extract_and_load(download_params: dict) -> None:
    _pipeline.run_extract_and_load(download_params)


extract_and_load.deploy_tags = deploy_tags(DATASET_ID, Etapa.EXTRACT_AND_LOAD)
