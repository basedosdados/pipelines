"""
Flows de br_ms_sim — Prefect 3.
"""

from prefect import flow

from pipelines.datasets.br_ms_sim.constants import constants
from pipelines.datasets.br_ms_sim.tasks import (
    clean_table,
    download_table,
    get_source_max_year,
    resolve_year_source,
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

DATE_FORMAT = DateFormat.YEAR


def coverage(table_id: str) -> CoverageSpec:
    """Devolve a cobertura da tabela.

    Raises:
        ValueError: Se a tabela não constar de `constants.TABLES`.
    """
    if table_id not in constants.TABLES.value:
        raise ValueError(f"tabela sem cobertura definida: {table_id}")
    return AllFree(
        date_column=YearOnly(col="ano"),
        date_format=DATE_FORMAT,
    )


def run_ms_sim(
    *,
    dataset_id: str,
    table_id: str,
    ano: int | None,
    materialize_after_dump: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
    dump_mode: str,
    source_format: str,
) -> None:
    """Executa o ciclo baixar, limpar, subir, dbt e metadados de um ano."""
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    backfill = ano is not None
    source_max_year = get_source_max_year()
    ano = int(ano if backfill else source_max_year)

    if not force_run and not backfill:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=source_max_year,
            env="prod",
            date_format=DATE_FORMAT,
            compare_against="coverage",
        )
        if not has_new_data:
            print(f"Não há atualizações para a tabela {table_id}!")
            return

    if not backfill:
        commit_source_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=source_max_year,
            env="prod",
            date_format=DATE_FORMAT,
            update_metadata=update_metadata,
            materialize_after_dump=materialize_after_dump,
        )

    source = resolve_year_source(ano)
    print(f"Carregando {ano} a partir do diretório {source}")

    download_table(table_id=table_id, ano=ano, source=source)
    filepath = clean_table(table_id=table_id, ano=ano, source=source)

    upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
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
        target=target,
    )

    if update_metadata:
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=table_id,
            coverage=coverage(table_id),
            env="prod",
            bq_project="basedosdados",
        )


def ms_sim_flow(
    table_id: str,
    dump_mode: str = "append",
    source_format: str = "csv",
):
    """Carimba o flow de uma tabela."""

    @flow(
        name=f"br_ms_sim__{table_id}",
        log_prints=True,
    )
    def table_flow(
        dataset_id: str = "br_ms_sim",
        table_id: str = table_id,
        ano: int | None = None,
        materialize_after_dump: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
    ) -> None:
        """Carrega um ano do SIM, do FTP do DATASUS até a materialização."""
        run_ms_sim(
            dataset_id=dataset_id,
            table_id=table_id,
            ano=ano,
            materialize_after_dump=materialize_after_dump,
            update_metadata=update_metadata,
            target=target,
            force_run=force_run,
            dump_mode=dump_mode,
            source_format=source_format,
        )

    return table_flow


br_ms_sim__microdados = ms_sim_flow("microdados")
