"""
Flow de teste end-to-end para validar o pipeline completo:
download → upload staging → dbt dev (run/test) → upload prod → dbt prod (run).

Não inclui atualização de metadados.
"""

from prefect import flow

from pipelines.datasets.test_dataset.tasks import download_taxa_cambio
from pipelines.utils.tasks import run_dbt, upload_to_gcs

DATASET_ID = "test_dataset"
TABLE_ID = "taxa_cambio"


@flow(
    name="test_dataset: taxa_cambio (end-to-end)",
    flow_run_name="test end-to-end: taxa_cambio",
    log_prints=True,
)
def test_taxa_cambio_flow(
    n_days: int = 30,
    materialize_after_dump: bool = True,
    target: str = "prod",
) -> None:
    filepath = download_taxa_cambio(n_days=n_days)

    upload_to_gcs(
        data_path=filepath,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        bucket_name="basedosdados-dev",
        dump_mode="overwrite",
    )

    run_dbt(
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        dbt_command="run/test",
        target="dev",
    )

    if not materialize_after_dump:
        return

    upload_to_gcs(
        data_path=filepath,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        bucket_name="basedosdados",
        dump_mode="overwrite",
    )

    run_dbt(
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        dbt_command="run",
        target=target,
    )


test_taxa_cambio_flow.deploy_schedules = []
