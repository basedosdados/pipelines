"""Flows for us_cfpb_hmda - Prefect 3.

Migrado pro pipeline orientado a eventos (issue #1867): check_update ->
download -> mat_test. Lógica específica do dataset mora em `tasks.py`
(`check_for_update`/`download_data`) — aqui só a fiação
(`CheckThenDownloadPipeline` + `@flow`).
"""

from prefect import flow

from pipelines.datasets.us_cfpb_hmda.constants import constants
from pipelines.datasets.us_cfpb_hmda.tasks import (
    check_for_update,
    download_data,
)
from pipelines.utils.stage_dispatch import (
    CheckThenDownloadPipeline,
    Etapa,
    deploy_tags,
)

DATASET_ID = constants.DATASET_ID.value
TABLE_ID = constants.TABLE_ID.value

_pipeline = CheckThenDownloadPipeline(
    dataset_id=DATASET_ID,
    table_id=TABLE_ID,
    check_for_update=check_for_update,
    download_data=download_data,
    # Cobertura anual (`YearOnly`) — mesma granularidade do flow antigo.
    date_format="%Y",
)


@flow(name=_pipeline.check_update_flow_name, log_prints=True)
def us_cfpb_hmda_check_update_flow() -> None:
    _pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
us_cfpb_hmda_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)
# CFPB publica o Snapshot anual ~meados de ano; sonda alguns dias por mês
# entre mar-ago (mesmo cron do flow antigo) — o check_for_update é barato
# (streaming, poucos KB), então roda com folga.
# pyrefly: ignore [missing-attribute]
us_cfpb_hmda_check_update_flow.deploy_schedules = [
    {"cron": "25 16 8,9,10 3,4,5,6,7,8 *", "timezone": "America/Sao_Paulo"}
]


@flow(name=_pipeline.download_flow_name, log_prints=True)
def us_cfpb_hmda_download_flow(download_params: dict) -> None:
    _pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
us_cfpb_hmda_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
# Reconstrói todo o histórico (FIRST_YEAR..max_year) a cada run — vários GB
# por ano; mesmo `job_variables` do flow monolítico antigo, agora isolado
# só nesta etapa (check_update não precisa dessa memória).
# pyrefly: ignore [missing-attribute]
us_cfpb_hmda_download_flow.job_variables = {"memory": "8Gi"}
_pipeline.download_deployment = us_cfpb_hmda_download_flow.fn.__name__
