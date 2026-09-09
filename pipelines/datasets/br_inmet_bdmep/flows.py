"""
Flows para br_inmet_bdmep — Prefect 3.

Migrado por completo pro pipeline orientado a eventos (issue #1867):
check_update -> download -> mat_test. Lógica específica do dataset mora em
`tasks.py`, constantes em `constants.py` — aqui só a fiação
(`CheckThenDownloadPipeline` + `@flow`).

O antigo flow monolítico (`br_inmet_bdmep__microdados`, cron às 22h de
seg-sex) foi removido deste arquivo.
"""

from prefect import flow

from pipelines.datasets.br_inmet_bdmep.constants import (
    DATASET_ID,
    MICRODADOS_TABLE_ID,
)
from pipelines.datasets.br_inmet_bdmep.tasks import (
    microdados_check_for_update,
    microdados_download,
)
from pipelines.utils.stage_dispatch import (
    CheckThenDownloadPipeline,
    Etapa,
    deploy_tags,
)

_microdados_pipeline = CheckThenDownloadPipeline(
    dataset_id=DATASET_ID,
    table_id=MICRODADOS_TABLE_ID,
    check_for_update=microdados_check_for_update,
    download_data=microdados_download,
    # Mesma granularidade do flow antigo (comparava coverage com
    # date_format="%Y-%m").
    date_format="%Y-%m",
)


@flow(name=_microdados_pipeline.check_update_flow_name, log_prints=True)
def br_inmet_bdmep_microdados_check_update_flow() -> None:
    _microdados_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_inmet_bdmep_microdados_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_microdados_pipeline.download_flow_name, log_prints=True)
def br_inmet_bdmep_microdados_download_flow(download_params: dict) -> None:
    _microdados_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_inmet_bdmep_microdados_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_microdados_pipeline.download_deployment = (
    br_inmet_bdmep_microdados_download_flow.fn.__name__
)
