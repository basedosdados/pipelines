"""
Flow br_ibge_pnadc — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.ibge_pnadc.tasks import (
    build_partitions,
    build_table_paths,
    get_data_source_date_and_url,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    YearQuarter,
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
from pipelines.utils.to_download.tasks import download_async


@flow(
    name="br_ibge_pnadc__microdados",
    log_prints=True,
)
def br_ibge_pnadc__microdados(
    dataset_id: str = "br_ibge_pnadc",
    table_id: str = "microdados",
    materialize_after_dump: bool = False,
    dbt_alias: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    data_source_max_date, url = get_data_source_date_and_url()

    if not force_run:
        outdated = register_source_poll_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=data_source_max_date,
            env="prod",
            date_format="%Y-%m-%d",
        )
        if not outdated:
            return

    input_dir, output_dir = build_table_paths(table_id=table_id)
    download_async(url, input_dir, "zip")
    build_partitions(input_dir, output_dir)

    upload_to_gcs(
        data_path=output_dir,
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
        data_path=output_dir,
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
            coverage=AllFree(
                date_column=YearQuarter(year="ano", quarter="trimestre"),
                date_format=DateFormat.YEAR_MONTH,
            ),
            env="prod",
            bq_project="basedosdados",
        )


br_ibge_pnadc__microdados.deploy_schedules = [
    {"cron": "0 5 15-31 2,5,8,11 *", "timezone": "America/Sao_Paulo"}
]
