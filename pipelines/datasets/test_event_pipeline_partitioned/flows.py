"""
Flows for test_event_pipeline_partitioned — Prefect 3.

Variante do piloto `test_event_pipeline` (issue #1867) pra testar dados
particionados (`ano=/mes=`) de ponta a ponta — o piloto original usa um
único arquivo sem partição, então nunca exercitou `partition_folders`
(`DownloadResult`) nem o caminho de `transfer_files_to_prod_flow` que
promove só a fatia particionada, não o staging inteiro.

Mesma estrutura do piloto original: `CheckThenDownloadPipeline`
(`pipelines/utils/stage_dispatch.py`), lógica específica do dataset em
`tasks.py`, `@flow` finos aqui.
"""

from prefect import flow

from pipelines.datasets.test_event_pipeline_partitioned.constants import (
    BACKEND_ENV,
    DATASET_ID,
    JOB_VARIABLES,
    PREFECT_DATASET_ID,
    TABLE_ID,
)
from pipelines.datasets.test_event_pipeline_partitioned.tasks import (
    check_update,
    download,
)
from pipelines.utils.stage_dispatch import (
    CheckThenDownloadPipeline,
    Etapa,
    deploy_tags,
)

_pipeline = CheckThenDownloadPipeline(
    dataset_id=DATASET_ID,
    table_id=TABLE_ID,
    prefect_dataset_id=PREFECT_DATASET_ID,
    env=BACKEND_ENV,
    check_fn=check_update,
    download_fn=download,
)


@flow(name="test_event_pipeline_partitioned: check_update", log_prints=True)
def check_update_flow() -> None:
    """Disparado manualmente pro teste. Todo o corpo mora em `_pipeline`."""
    _pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
check_update_flow.deploy_tags = deploy_tags(
    PREFECT_DATASET_ID, Etapa.CHECK_UPDATE
)
# pyrefly: ignore [missing-attribute]
check_update_flow.job_variables = JOB_VARIABLES[Etapa.CHECK_UPDATE]


@flow(name="test_event_pipeline_partitioned: flow_download", log_prints=True)
def flow_download_flow(download_params: dict) -> None:
    """
    Disparado por `check_update_flow` via `run_deployment()`, ou
    manualmente (rerun/debug) passando `download_params` à mão.
    """
    _pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
flow_download_flow.deploy_tags = deploy_tags(
    PREFECT_DATASET_ID, Etapa.FLOW_DOWNLOAD
)
# pyrefly: ignore [missing-attribute]
flow_download_flow.job_variables = JOB_VARIABLES[Etapa.FLOW_DOWNLOAD]
