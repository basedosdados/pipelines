"""
Flows de br_inep_enem — Prefect 3.

Um `@flow` por tabela, declarado no nível do módulo com o horário logo abaixo,
compartilhando a espinha `run_inep_enem`.
"""

from prefect import flow

from pipelines.datasets.br_inep_enem.tasks import (
    clean_table,
    download_table,
    get_source_max_date,
    resolve_years,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    CoverageSpec,
    DateFormat,
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

COVERAGE: dict[str, CoverageSpec] = {
    "participantes": AllFree(
        date_column=YearOnly(col="ano"), date_format=DateFormat.YEAR
    ),
    "resultados": AllFree(
        date_column=YearOnly(col="ano"), date_format=DateFormat.YEAR
    ),
}


def run_inep_enem(
    *,
    dataset_id: str,
    table_id: str,
    materialize_after_dump: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
    backfill_years: list[str] | None,
    dump_mode: str,
    source_format: str,
) -> None:
    """Executa o ciclo baixar, limpar, subir, dbt e metadados de uma tabela."""
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    source_max_date = get_source_max_date()

    if not backfill_years:
        if not force_run:
            has_new_data = poll_source_for_update_task(
                dataset_id=dataset_id,
                table_id=table_id,
                source_max_date=source_max_date,
                env="prod",
                date_format=DateFormat.YEAR,
                compare_against="coverage",
            )
            if not has_new_data:
                print(f"Não há atualizações para a tabela {table_id}!")
                return

        commit_source_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=source_max_date,
            env="prod",
            date_format=DateFormat.YEAR,
            update_metadata=update_metadata,
            materialize_after_dump=materialize_after_dump,
        )

    anos = resolve_years(
        backfill_years=backfill_years, source_max_date=source_max_date
    )

    for ano in anos:
        download_table(table_id=table_id, ano=ano)
        filepath = clean_table(table_id=table_id, ano=ano)

        upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            bucket_name="basedosdados-dev",
            dump_mode=dump_mode,
            source_format=source_format,
        )

        if materialize_after_dump:
            upload_to_gcs(
                data_path=filepath,
                dataset_id=dataset_id,
                table_id=table_id,
                bucket_name="basedosdados",
                dump_mode=dump_mode,
                source_format=source_format,
            )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        target="dev",
    )

    if not materialize_after_dump:
        return

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
            coverage=COVERAGE[table_id],
            env="prod",
            bq_project="basedosdados",
        )


@flow(
    name="br_inep_enem__participantes",
    log_prints=True,
)
def br_inep_enem__participantes(
    dataset_id: str = "br_inep_enem",
    table_id: str = "participantes",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
    backfill_years: list[str] | None = None,
) -> None:
    """Atualiza os participantes com a edição mais recente da fonte."""
    run_inep_enem(
        dataset_id=dataset_id,
        table_id=table_id,
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
        backfill_years=backfill_years,
        dump_mode="append",
        source_format="parquet",
    )


# pyrefly: ignore [missing-attribute]
br_inep_enem__participantes.deploy_schedules = [
    {"cron": "13 9 12,13,14 * *", "timezone": "America/Sao_Paulo"}
]


@flow(
    name="br_inep_enem__resultados",
    log_prints=True,
)
def br_inep_enem__resultados(
    dataset_id: str = "br_inep_enem",
    table_id: str = "resultados",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
    backfill_years: list[str] | None = None,
) -> None:
    """Atualiza os resultados com a edição mais recente da fonte."""
    run_inep_enem(
        dataset_id=dataset_id,
        table_id=table_id,
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
        backfill_years=backfill_years,
        dump_mode="append",
        source_format="parquet",
    )


# pyrefly: ignore [missing-attribute]
br_inep_enem__resultados.deploy_schedules = [
    {"cron": "43 9 12,13,14 * *", "timezone": "America/Sao_Paulo"}
]

# pyrefly: ignore [missing-attribute]
br_inep_enem__resultados.job_variables = {
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
