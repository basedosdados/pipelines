"""
Flows para br_me_caged — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.me_caged.tasks import (
    build_partitions,
    build_table_paths,
    crawl_novo_caged_ftp,
    generate_yearmonth_range,
    get_source_last_date,
    get_table_last_date,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    PartBdpro,
    YearMonth,
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


def _run_me_caged(
    *,
    dataset_id: str,
    table_id: str,
    materialize_after_dump: bool,
    dbt_alias: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
) -> None:
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    source_last_date = get_source_last_date()

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=source_last_date,
            env="prod",
            date_format="%Y-%m",
        )
        if not has_new_data:
            print(f"No updates for table {table_id}!")
            return

    table_last_date = get_table_last_date(dataset_id, table_id)
    _input_dir, output_dir = build_table_paths(table_id)
    yearmonths = generate_yearmonth_range(table_last_date, source_last_date)

    for ym in yearmonths:
        failed = crawl_novo_caged_ftp(ym, table_id)
        print(f"crawl {ym} → failed_downloads={failed}")

    filepath = build_partitions(table_id=table_id, table_output_dir=output_dir)

    upload_to_gcs(
        data_path=filepath,
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
        data_path=filepath,
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
                date_column=YearMonth(year="ano", month="mes"),
                date_format=DateFormat.YEAR_MONTH,
            ),
            env="prod",
            bq_project="basedosdados",
        )

        if source_last_date is not None:
            commit_source_update_task(
                dataset_id=dataset_id,
                table_id=table_id,
                source_max_date=source_last_date,
                env="prod",
                date_format="%Y-%m",
            )


def _caged_flow(table_id: str, cron: str):
    @flow(
        name=f"br_me_caged__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_me_caged",
        table_id: str = table_id,
        materialize_after_dump: bool = True,
        dbt_alias: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
    ) -> None:
        _run_me_caged(
            dataset_id=dataset_id,
            table_id=table_id,
            materialize_after_dump=materialize_after_dump,
            dbt_alias=dbt_alias,
            update_metadata=update_metadata,
            target=target,
            force_run=force_run,
        )

    # pyrefly: ignore [missing-attribute]
    _flow.deploy_schedules = [{"cron": cron, "timezone": "America/Sao_Paulo"}]
    return _flow


br_me_caged__microdados_movimentacao = _caged_flow(
    "microdados_movimentacao", "0 8,17 1-4,26-31 * *"
)
br_me_caged__microdados_movimentacao_fora_prazo = _caged_flow(
    "microdados_movimentacao_fora_prazo", "0 8,17 1-4,26-31 * *"
)
br_me_caged__microdados_movimentacao_excluida = _caged_flow(
    "microdados_movimentacao_excluida", "0 8,17 1-4,26-31 * *"
)
