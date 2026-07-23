"""
Flow br_cnj_improbidade_administrativa — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.cnj_improbidade_administrativa.tasks import (
    get_max_date,
    is_up_to_date,
    main_task,
    write_csv_file,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    PartBdpro,
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
    name="br_cnj_improbidade_administrativa__condenacao",
    log_prints=True,
)
def br_cnj_improbidade_administrativa__condenacao(
    dataset_id: str = "br_cnj_improbidade_administrativa",
    table_id: str = "condenacao",
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

    if not force_run and is_up_to_date():
        print("Data already updated")
        return

    df = main_task()
    _ = get_max_date(df)
    output_filepath = write_csv_file(df)

    upload_to_gcs(
        data_path=output_filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="overwrite",
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
        dump_mode="overwrite",
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
                date_column=DateOnly(col="data_propositura"),
                date_format=DateFormat.YEAR_MD,
            ),
            env="prod",
            bq_project="basedosdados",
        )


# pyrefly: ignore [missing-attribute]
br_cnj_improbidade_administrativa__condenacao.deploy_schedules = [
    {"cron": "0 7 * * 1", "timezone": "America/Sao_Paulo"}
]
