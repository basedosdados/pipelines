"""Flows for us_cfpb_hmda - Prefect 3.

Migrado pro pipeline orientado a eventos (issue #1867): check_update ->
extract_and_load -> build_and_promote. Lógica específica do dataset mora em `tasks.py`
(`get_latest_update`/`extract_load_data`) — aqui só a fiação
(`CheckThenExtractLoadPipeline` + `@flow`).
"""

from prefect.schedules import Cron

from pipelines.datasets.us_cfpb_hmda.constants import constants
from pipelines.datasets.us_cfpb_hmda.tasks import (
    extract_load_data,
    get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import (
    CheckThenExtractLoadPipeline,
    Etapa,
    deploy_tags,
)

DATASET_ID = constants.DATASET_ID.value
TABLE_ID = constants.TABLE_ID.value

_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=TABLE_ID,
    get_latest_update=get_latest_update,
    extract_load_data=extract_load_data,
    # Cobertura anual (`YearOnly`) — mesma granularidade do flow antigo.
    date_format="%Y",
)


@flow(name=_pipeline.check_update_flow_name, log_prints=True)
def us_cfpb_hmda_check_update() -> None:
    _pipeline.run_check_update()


us_cfpb_hmda_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)
# CFPB publica o Snapshot anual ~meados de ano; sonda alguns dias por mês
# entre mar-ago (mesmo cron do flow antigo) — o get_latest_update é barato
# (streaming, poucos KB), então roda com folga.
us_cfpb_hmda_check_update.deploy_schedules = [
    Cron("25 16 8,9,10 3,4,5,6,7,8 *", timezone="America/Sao_Paulo")
]


@flow(name=_pipeline.extract_and_load_flow_name, log_prints=True)
def us_cfpb_hmda_download(download_params: dict) -> None:
    _pipeline.run_extract_and_load(download_params)


us_cfpb_hmda_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD
)
# Reconstrói todo o histórico (FIRST_YEAR..max_year) a cada run — vários GB
# por ano; mesmo `job_variables` do flow monolítico antigo, agora isolado
# só nesta etapa (check_update não precisa dessa memória).
us_cfpb_hmda_download.job_variables = {"memory": "8Gi"}
_pipeline.extract_load_deployment = us_cfpb_hmda_download.fn.__name__
