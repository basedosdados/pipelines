"""
Flow br_bcb_taxa_cambio — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.bcb_taxa_cambio.tasks import (
    get_data_taxa_cambio,
    treat_data_taxa_cambio,
)
from pipelines.utils.metadata.domain import (
    AllBdpro,
    DateFormat,
    DateOnly,
)
from pipelines.utils.metadata.tasks import (
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)


@flow(
    name="br_bcb_taxa_cambio__taxa_cambio",
    log_prints=True,
)
def br_bcb_taxa_cambio__taxa_cambio(
    dataset_id: str = "br_bcb_taxa_cambio",
    table_id: str = "taxa_cambio",
    materialize_after_dump: bool = True,
    dbt_alias: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    get_data_taxa_cambio(table_id=table_id)
    file_info = treat_data_taxa_cambio(table_id=table_id)

    upload_to_gcs(
        # pyrefly: ignore [bad-index]
        data_path=file_info["save_output_path"],
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="append",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target="dev",
    )

    if not materialize_after_dump:
        return

    upload_to_gcs(
        # pyrefly: ignore [bad-index]
        data_path=file_info["save_output_path"],
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados",
        dump_mode="append",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target=target,
    )

    if update_metadata:
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=table_id,
            coverage=AllBdpro(
                date_column=DateOnly(col="data_cotacao"),
                date_format=DateFormat.YEAR_MD,
            ),
            env="prod",
            bq_project="basedosdados",
        )


# pyrefly: ignore [missing-attribute]
br_bcb_taxa_cambio__taxa_cambio.deploy_schedules = [
    {"cron": "0 8 * * *", "timezone": "America/Sao_Paulo"}
]
