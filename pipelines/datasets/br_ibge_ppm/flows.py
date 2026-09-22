"""
Flows de br_ibge_ppm — Prefect 3.

Um `@flow` por tabela, declarado no nível do módulo com o horário logo abaixo,
compartilhando a espinha `run_ibge_ppm`.
"""

from prefect import flow

from pipelines.datasets.br_ibge_ppm.tasks import (
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
    "efetivo_rebanhos": AllFree(
        date_column=YearOnly(col="ano"), date_format=DateFormat.YEAR
    ),
    "producao_origem_animal": AllFree(
        date_column=YearOnly(col="ano"), date_format=DateFormat.YEAR
    ),
    "producao_aquicultura": AllFree(
        date_column=YearOnly(col="ano"), date_format=DateFormat.YEAR
    ),
    "producao_pecuaria": AllFree(
        date_column=YearOnly(col="ano"), date_format=DateFormat.YEAR
    ),
}


def run_ibge_ppm(
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

    source_max_date = get_source_max_date(table_id=table_id)

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
        table_id=table_id,
        backfill_years=backfill_years,
        source_max_date=source_max_date,
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
    name="br_ibge_ppm__efetivo_rebanhos",
    log_prints=True,
)
def br_ibge_ppm__efetivo_rebanhos(
    dataset_id: str = "br_ibge_ppm",
    table_id: str = "efetivo_rebanhos",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
    backfill_years: list[str] | None = None,
) -> None:
    """Atualiza o efetivo dos rebanhos com o ano mais recente da fonte."""
    run_ibge_ppm(
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
br_ibge_ppm__efetivo_rebanhos.deploy_schedules = [
    {"cron": "4 10 15-20 9,10 *", "timezone": "America/Sao_Paulo"}
]


@flow(
    name="br_ibge_ppm__producao_origem_animal",
    log_prints=True,
)
def br_ibge_ppm__producao_origem_animal(
    dataset_id: str = "br_ibge_ppm",
    table_id: str = "producao_origem_animal",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
    backfill_years: list[str] | None = None,
) -> None:
    """Atualiza a produção de origem animal com o ano mais recente da fonte."""
    run_ibge_ppm(
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
br_ibge_ppm__producao_origem_animal.deploy_schedules = [
    {"cron": "9 10 15-20 9,10 *", "timezone": "America/Sao_Paulo"}
]


@flow(
    name="br_ibge_ppm__producao_aquicultura",
    log_prints=True,
)
def br_ibge_ppm__producao_aquicultura(
    dataset_id: str = "br_ibge_ppm",
    table_id: str = "producao_aquicultura",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
    backfill_years: list[str] | None = None,
) -> None:
    """Atualiza a produção da aquicultura com o ano mais recente da fonte."""
    run_ibge_ppm(
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
br_ibge_ppm__producao_aquicultura.deploy_schedules = [
    {"cron": "14 10 15-20 9,10 *", "timezone": "America/Sao_Paulo"}
]


@flow(
    name="br_ibge_ppm__producao_pecuaria",
    log_prints=True,
)
def br_ibge_ppm__producao_pecuaria(
    dataset_id: str = "br_ibge_ppm",
    table_id: str = "producao_pecuaria",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
    backfill_years: list[str] | None = None,
) -> None:
    """Atualiza a produção pecuária com o ano mais recente da fonte."""
    run_ibge_ppm(
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
br_ibge_ppm__producao_pecuaria.deploy_schedules = [
    {"cron": "19 10 15-20 9,10 *", "timezone": "America/Sao_Paulo"}
]
