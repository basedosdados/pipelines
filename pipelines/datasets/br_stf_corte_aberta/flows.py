"""
Flows para br_stf_corte_aberta — Prefect 3.

Migrado por completo pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote. Lógica específica do
dataset mora em `tasks.py` (que reaproveita `pipelines.crawler.stf_corte_aberta`),
constantes em `constants.py` — aqui só a fiação (`CheckThenExtractLoadPipeline`
+ `@flow`).

Dataset com uma tabela só (`decisoes`) — ver `tasks.py` sobre o Selenium
disparando download tanto no check_update quanto no extract_and_load (custo
duplicado aceito, variante "check_and_download" foi descartada).
"""

from prefect.schedules import Cron

from pipelines.datasets.br_stf_corte_aberta.constants import (
    DATASET_ID,
    DECISOES_TABLE_ID,
)
from pipelines.datasets.br_stf_corte_aberta.tasks import (
    extract_load_data,
    get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import (
    CheckThenExtractLoadPipeline,
    Etapa,
    deploy_tags,
)

_decisoes_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=DECISOES_TABLE_ID,
    get_latest_update=get_latest_update,
    extract_load_data=extract_load_data,
)


@flow(name=_decisoes_pipeline.check_update_flow_name, log_prints=True)
def br_stf_corte_aberta_decisoes_check_update() -> None:
    _decisoes_pipeline.run_check_update()


br_stf_corte_aberta_decisoes_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, DECISOES_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_stf_corte_aberta_decisoes_check_update.deploy_schedules = [
    Cron("0 12 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_decisoes_pipeline.extract_and_load_flow_name, log_prints=True)
def br_stf_corte_aberta_decisoes_download(download_params: dict) -> None:
    _decisoes_pipeline.run_extract_and_load(download_params)


br_stf_corte_aberta_decisoes_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, DECISOES_TABLE_ID
)
_decisoes_pipeline.extract_load_deployment = (
    br_stf_corte_aberta_decisoes_download.fn.__name__
)
