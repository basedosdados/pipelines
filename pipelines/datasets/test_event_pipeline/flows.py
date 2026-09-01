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

Cadeia: check_update -> (run_deployment) -> flow_download -> (run_deployment) -> mat_test (genérico).
Cada etapa dispara a próxima direto no código
(`pipelines/utils/stage_dispatch.py`).
"""

from datetime import UTC, datetime

from prefect import flow
from prefect.utilities.asyncutils import run_coro_as_sync

from pipelines.datasets.test_event_pipeline.constants import (
    BACKEND_ENV,
    DATASET_ID,
    PREFECT_DATASET_ID,
    TABLE_ID,
)
from pipelines.datasets.test_event_pipeline.tasks import (
    dispatch_mat_test,
    write_reference_date_csv,
)
from pipelines.utils.metadata.domain import AllFree, DateFormat, DateOnly
from pipelines.utils.stage_dispatch import (
    check_update_and_dispatch,
    deploy_tags,
)
from pipelines.utils.tasks import rename_flow_run_dataset_table, upload_to_gcs


@flow(name="test_event_pipeline: check_update", log_prints=True)
def check_update_flow() -> None:
    """
    Checagem real: compara a data de hoje contra a coverage registrada no
    backend pra `test_dataset.test_event_pipeline`. Todo o poll + commit +
    dispatch fica em `check_update_and_dispatch`
    (`pipelines/utils/stage_dispatch.py`) — outros datasets com o mesmo
    padrão de check_update chamam o mesmo helper, sem repetir essa
    sequência em cada flow.
    """
    run_coro_as_sync(
        rename_flow_run_dataset_table(
            prefix="Check Update: ",
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
        )
    )

    reference_date = datetime.now(UTC).date()

    check_update_and_dispatch(
        prefect_dataset_id=PREFECT_DATASET_ID,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        reference_date=reference_date,
        next_etapa="flow_download",
        env=BACKEND_ENV,
    )


# pyrefly: ignore [missing-attribute]
check_update_flow.deploy_tags = deploy_tags(PREFECT_DATASET_ID, "check_update")


@flow(name="test_event_pipeline: flow_download", log_prints=True)
def flow_download_flow(download_params: dict) -> None:
    """
    Disparado por `check_update_and_dispatch` via `run_deployment()`, ou
    manualmente (rerun/debug, sem passar pelo check_update) passando o
    dict à mão. Só baixa (simulado: cria um CSV pequeno com a data de
    referência) e atualiza o staging (`upload_to_gcs`) — a materialização
    de verdade fica a cargo do `mat_test_flow` genérico, disparado ao
    final com tudo que ele precisa pra rodar sem saber nada sobre este
    dataset de antemão (dataset_id, table_id, coverage).
    """
    run_coro_as_sync(
        rename_flow_run_dataset_table(
            prefix="Flow Download: ",
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
        )
    )

    reference_date = download_params["reference_date"]

    csv_path = write_reference_date_csv(reference_date=reference_date)
    upload_to_gcs(
        data_path=csv_path,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        bucket_name="basedosdados-dev",
        dump_mode="append",
    )

    dispatch_mat_test(
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        coverage=AllFree(
            date_column=DateOnly(col="reference_date"),
            date_format=DateFormat.YEAR_MD,
        ).model_dump(),
        env=BACKEND_ENV,
        bq_project="basedosdados-dev",
        prefect_mode="dev",
        # Teste real do caminho dev->prod (issue #1867): mat_test_flow
        # roda no pool basedosdados (prod), então "prod" aqui exercita
        # transfer_files_to_prod_flow de verdade.
        targets=["dev", "prod"],
    )


# pyrefly: ignore [missing-attribute]
flow_download_flow.deploy_tags = deploy_tags(
    PREFECT_DATASET_ID, "flow_download"
)
