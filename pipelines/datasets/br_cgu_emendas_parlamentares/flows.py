"""
Flows para br_cgu_emendas_parlamentares — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.cgu_emendas_parlamentares.tasks import (
    convert_str_to_float,
    get_last_modified_time,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    FreeLag,
    PartBdpro,
    YearOnly,
)
from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    poll_source_for_update_task,
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)


@flow(
    name="br_cgu_emendas_parlamentares__microdados",
    log_prints=True,
)
def br_cgu_emendas_parlamentares__microdados(
    dataset_id: str = "br_cgu_emendas_parlamentares",
    table_id: str = "microdados",
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

    max_modified_time = get_last_modified_time()

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=max_modified_time,
            env="prod",
            date_format="%Y-%m-%d",
        )
        if not has_new_data:
            return

    output_path = convert_str_to_float()

    upload_to_gcs(
        data_path=output_path,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="append",
        source_format="csv",
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
        data_path=output_path,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados",
        dump_mode="append",
        source_format="csv",
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
                date_column=YearOnly(col="ano_emenda"),
                date_format=DateFormat.YEAR,
                free_lag=FreeLag(unit="years", value=1),
            ),
            env="prod",
            bq_project="basedosdados",
        )

        if max_modified_time is not None:
            commit_source_update_task(
                dataset_id=dataset_id,
                table_id=table_id,
                source_max_date=max_modified_time,
                env="prod",
                date_format="%Y-%m-%d",
            )


# pyrefly: ignore [missing-attribute]
br_cgu_emendas_parlamentares__microdados.deploy_schedules = [
    {"cron": "30 19 * * *", "timezone": "America/Sao_Paulo"}
]
