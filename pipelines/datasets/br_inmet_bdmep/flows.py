"""
Flows for br_inmet_bdmep — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.inmet_bdmep.tasks import (
    extract_last_date_from_source,
    get_base_inmet,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    PartBdpro,
)
from pipelines.utils.metadata.tasks import (
    register_source_poll_task,
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)


@flow(
    name="br_inmet_bdmep__microdados",
    log_prints=True,
)
def br_inmet_bdmep__microdados(
    dataset_id: str = "br_inmet_bdmep",
    table_id: str = "microdados",
    materialize_after_dump: bool = True,
    dbt_alias: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    source_last_date = extract_last_date_from_source()

    if not force_run:
        is_outdated = register_source_poll_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=source_last_date,
            env="prod",
            date_format="%Y-%m",
        )
        if not is_outdated:
            return

    output_filepath = get_base_inmet()

    upload_to_gcs(
        data_path=output_filepath,
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
        data_path=output_filepath,
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
            coverage=PartBdpro(
                date_column=DateOnly(col="data"),
                date_format=DateFormat.YEAR_MD,
            ),
            env="prod",
            bq_project="basedosdados",
        )


br_inmet_bdmep__microdados.deploy_schedules = [
    {"cron": "0 22 * * 1-5", "timezone": "America/Sao_Paulo"},
]
