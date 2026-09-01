"""
Flows for test_event_pipeline — Prefect 3.

Piloto da issue #1867, agora com um check_update real (não mais simulado):
`check_update_flow` compara a data de hoje contra a coverage registrada no
backend pra `test_dataset.test_event_pipeline` — mesmo padrão usado por
datasets reais (`pipelines/utils/metadata/tasks.py::poll_source_for_update_task`).
`flow_download_flow` simula um download (cria um CSV pequeno) e sobe pro
staging via `upload_to_gcs`. A materialização de verdade (dbt run/test em
dev e prod + atualização da coverage no backend) é feita pelo
`mat_test_flow` **genérico** (`pipelines/utils/metadata/flows.py`) — não um
`mat_test_flow` por dataset, já que essa etapa é sempre a mesma sequência
pra qualquer tabela, só os parâmetros mudam.

Cadeia: check_update -> (Automação 1) -> flow_download -> (Automação 2) -> mat_test (genérico).
"""

from datetime import UTC, datetime

from prefect import flow

from pipelines.datasets.test_event_pipeline.constants import (
    BACKEND_DATASET_ID,
    BACKEND_ENV,
    BACKEND_TABLE_ID,
    DATASET_ID,
)
from pipelines.datasets.test_event_pipeline.tasks import (
    emit_flow_download_completed,
    write_reference_date_csv,
)
from pipelines.utils.automations import (
    check_update_and_emit,
    decode_params,
    deploy_tags,
)
from pipelines.utils.metadata.domain import AllFree, DateFormat, DateOnly
from pipelines.utils.tasks import upload_to_gcs


@flow(name="test_event_pipeline: check_update", log_prints=True)
def check_update_flow() -> None:
    """
    Checagem real: compara a data de hoje contra a coverage registrada no
    backend pra `test_dataset.test_event_pipeline`. Todo o poll + commit +
    emit fica em `check_update_and_emit` (`pipelines/utils/automations.py`)
    — outros datasets com o mesmo padrão de check_update chamam o mesmo
    helper, sem repetir essa sequência em cada flow.
    """
    reference_date = datetime.now(UTC).date()

    check_update_and_emit(
        resource_dataset_id=DATASET_ID,
        backend_dataset_id=BACKEND_DATASET_ID,
        backend_table_id=BACKEND_TABLE_ID,
        reference_date=reference_date,
        upstream_etapa="check_update",
        env=BACKEND_ENV,
    )


# pyrefly: ignore [missing-attribute]
check_update_flow.deploy_tags = deploy_tags(DATASET_ID, "check_update")


@flow(name="test_event_pipeline: flow_download", log_prints=True)
def flow_download_flow(download_params: str) -> None:
    """
    Disparado pela Automação 1 via evento `check_update.completed`, ou
    manualmente (rerun/debug, sem passar pelo check_update) montando o JSON
    à mão. Só baixa (simulado: cria um CSV pequeno com a data de
    referência) e atualiza o staging (`upload_to_gcs`) — a materialização
    de verdade fica a cargo do `mat_test_flow` genérico, então o payload
    pra Automação 2 já vem com tudo que ele precisa pra rodar sem saber
    nada sobre este dataset de antemão (dataset_id, table_id, coverage).
    """
    params = decode_params(download_params)
    reference_date = params["reference_date"]

    csv_path = write_reference_date_csv(reference_date=reference_date)
    upload_to_gcs(
        data_path=csv_path,
        dataset_id=BACKEND_DATASET_ID,
        table_id=BACKEND_TABLE_ID,
        bucket_name="basedosdados-dev",
        dump_mode="append",
    )

    emit_flow_download_completed(
        mat_test_params={
            "dataset_id": BACKEND_DATASET_ID,
            "table_id": BACKEND_TABLE_ID,
            "coverage": AllFree(
                date_column=DateOnly(col="reference_date"),
                date_format=DateFormat.YEAR_MD,
            ).model_dump(),
            "env": BACKEND_ENV,
            "bq_project": "basedosdados-dev",
            "prefect_mode": "dev",
        }
    )


# pyrefly: ignore [missing-attribute]
flow_download_flow.deploy_tags = deploy_tags(DATASET_ID, "flow_download")
