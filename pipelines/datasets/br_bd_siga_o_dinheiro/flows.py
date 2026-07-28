"""
Flow br_bd_siga_o_dinheiro — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.bd_siga_o_dinheiro.tasks import get_table_ids
from pipelines.utils.tasks import download_data_to_gcs, run_dbt


@flow(
    name="br_bd_siga_o_dinheiro",
    log_prints=True,
)
def br_bd_siga_o_dinheiro(
    dataset_id: str = "br_bd_siga_o_dinheiro",
    dbt_alias: bool = True,
    target: str = "prod",
) -> None:
    table_ids = get_table_ids()

    for table_id in table_ids:
        run_dbt(
            dataset_id=dataset_id,
            table_id=table_id,
            dbt_command="run/test",
            dbt_alias=dbt_alias,
            target=target,
        )
        download_data_to_gcs(dataset_id=dataset_id, table_id=table_id)


br_bd_siga_o_dinheiro.deploy_schedules = []
