"""
Flows para br_me_caged — Prefect 3.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_me_caged.tasks import (
    build_partitions,
    build_table_paths,
    check_competence_month,
    check_municipality_codes,
    check_rows_written,
    check_states_present,
    download_caged_ftp,
    get_directory_municipality_codes,
    get_source_last_date,
    get_table_last_date,
    resolve_year_months,
)
from pipelines.utils.flow import flow
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
    update_metadata: bool,
    target: str,
    force_run: bool,
    backfill_year_months: list[str] | None,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    source_last_date = get_source_last_date()

    if not backfill_year_months:
        if not force_run:
            has_new_data = poll_source_for_update_task(
                dataset_id=dataset_id,
                table_id=table_id,
                source_max_date=source_last_date,
                env="prod",
                date_format="%Y-%m",
                compare_against="coverage",
            )
            if not has_new_data:
                print(f"No updates for table {table_id}!")
                return

        commit_source_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=source_last_date,
            env="prod",
            date_format="%Y-%m",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_after_dump,
        )

    table_last_date = get_table_last_date(dataset_id, table_id)
    year_months = resolve_year_months(
        backfill_year_months=backfill_year_months,
        table_last_date=table_last_date,
        source_last_date=source_last_date,
    )

    input_dir, output_dir = build_table_paths(table_id)

    for ym in year_months:
        failed = download_caged_ftp(ym, table_id)
        print(f"crawl {ym} → failed_downloads={failed}")

    filepath = build_partitions(table_id=table_id, table_output_dir=output_dir)

    check_rows_written(table_input_dir=input_dir, table_output_dir=output_dir)

    check_competence_month(
        table_id=table_id,
        table_input_dir=input_dir,
        table_output_dir=output_dir,
    )

    directory_codes = get_directory_municipality_codes()

    check_municipality_codes(
        table_input_dir=input_dir,
        table_output_dir=output_dir,
        directory_codes=directory_codes,
    )

    check_states_present(
        table_input_dir=input_dir, table_output_dir=output_dir
    )

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


def _caged_flow(table_id: str, cron: str):
    @flow(
        name=f"br_me_caged__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_me_caged",
        table_id: str = table_id,
        materialize_after_dump: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
        backfill_year_months: list[str] | None = None,
    ) -> None:
        _run_me_caged(
            dataset_id=dataset_id,
            table_id=table_id,
            materialize_after_dump=materialize_after_dump,
            update_metadata=update_metadata,
            target=target,
            force_run=force_run,
            backfill_year_months=backfill_year_months,
        )

    _flow.deploy_schedules = [Cron(cron, timezone="America/Sao_Paulo")]
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
