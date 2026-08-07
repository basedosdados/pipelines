"""
Flow br_stf_corte_aberta — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.stf_corte_aberta.tasks import (
    download_and_transform,
    get_data_source_stf_max_date,
    make_partitions,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    FreeLag,
    PartBdpro,
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
    name="br_stf_corte_aberta__decisoes",
    log_prints=True,
)
def br_stf_corte_aberta__decisoes(
    dataset_id: str = "br_stf_corte_aberta",
    table_id: str = "decisoes",
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

    data_source_max_date = get_data_source_stf_max_date()

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=data_source_max_date,
            env="prod",
            date_format="%Y-%m-%d",
        )
        if not has_new_data:
            return

    df = download_and_transform()
    output_path = make_partitions(df=df)

    upload_to_gcs(
        data_path=output_path,
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
        data_path=output_path,
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
                date_column=DateOnly(col="data_decisao"),
                date_format=DateFormat.YEAR_MD,
                free_lag=FreeLag(unit="weeks", value=6),
            ),
            env="prod",
            bq_project="basedosdados",
        )

        if data_source_max_date is not None:
            commit_source_update_task(
                dataset_id=dataset_id,
                table_id=table_id,
                source_max_date=data_source_max_date,
                env="prod",
                date_format="%Y-%m-%d",
            )


# pyrefly: ignore [missing-attribute]
br_stf_corte_aberta__decisoes.deploy_schedules = [
    {"cron": "0 12 * * *", "timezone": "America/Sao_Paulo"}
]
