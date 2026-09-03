"""
Flows for test_event_pipeline — Prefect 3.

Piloto da issue #1867, refatorado pra usar `CheckThenDownloadPipeline`
(`pipelines/utils/stage_dispatch.py`) — a interface recomendada pra
qualquer dataset novo na variante padrão (check_update e flow_download
separados). A lógica específica do dataset (`check_update`, `download`)
mora em `tasks.py`; este arquivo só monta o `_pipeline` e expõe os dois
`@flow` finos que o `deploy_flows.py` precisa encontrar aqui (não dá pra
gerar via fábrica genérica — ver docstring da classe).

`check_update_flow` compara a data de hoje contra a coverage registrada
no backend pra `test_dataset.test_event_pipeline` — mesmo padrão usado
por datasets reais (`pipelines/utils/metadata/tasks.py::poll_source_for_update_task`).
`flow_download_flow` simula um download (cria um CSV pequeno) e sobe pro
staging via `upload_to_gcs`. A materialização de verdade (dbt run/test em
dev e prod + atualização da coverage no backend) é feita pelo
`mat_test_flow` **genérico** (`pipelines/utils/metadata/flows.py`).

Cadeia: check_update -> (run_deployment) -> flow_download -> (run_deployment) -> mat_test (genérico).
"""

from prefect import flow

from pipelines.datasets.test_event_pipeline.constants import (
    BACKEND_ENV,
    DATASET_ID,
    JOB_VARIABLES,
    PREFECT_DATASET_ID,
    TABLE_ID,
)
from pipelines.datasets.test_event_pipeline.tasks import check_update, download
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


@flow(name="test_event_pipeline: check_update", log_prints=True)
def check_update_flow() -> None:
    """Disparado pelo schedule. Todo o corpo mora em `_pipeline` — ver
    `CheckThenDownloadPipeline` em `pipelines/utils/stage_dispatch.py`."""
    _pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
check_update_flow.deploy_tags = deploy_tags(
    PREFECT_DATASET_ID, Etapa.CHECK_UPDATE
)
# pyrefly: ignore [missing-attribute]
check_update_flow.job_variables = JOB_VARIABLES[Etapa.CHECK_UPDATE]


@flow(name="test_event_pipeline: flow_download", log_prints=True)
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
