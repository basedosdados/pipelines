"""
Tasks para br_bcb_sicor — Prefect 3.
"""

import pandas as pd
from prefect import task

from pipelines.constants import constants
from pipelines.datasets.br_bcb_sicor.utils import (
    build_sicor_download_df,
    create_dictionary,
    create_empreendimento,
    create_folder_structure,
    create_fonte_recurso,
    create_tables,
    filter_sicor_links,
    get_sicor_download_links,
)
from pipelines.utils.metadata.domain import (
    AllBdpro,
    AllFree,
    CoverageSpec,
    DateFormat,
    NonHistorical,
    PartBdpro,
    YearMonth,
)
from pipelines.utils.metadata.tasks import (
    commit_source_size_update_task,
    poll_source_size_for_update_task,
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

TASK_RETRIES = constants.TASK_MAX_RETRIES.value
TASK_RETRY_DELAY_SECONDS = constants.TASK_RETRY_DELAY.value


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def search_sicor_links() -> pd.DataFrame:
    """Scrape o site do BCB e devolve DataFrame com links e metadados."""
    links = get_sicor_download_links()
    return build_sicor_download_df(links)


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def get_sicor_table_size(
    links_df: pd.DataFrame, table_id: str, download_all_files: bool = False
) -> int:
    """Soma o `content_length` dos arquivos da tabela `table_id`."""
    table_df = filter_sicor_links(
        links_df, table_id, download_all_files=download_all_files
    )

    if table_df.empty:
        raise ValueError(
            f"No links found for table {table_id}. Check the sicor website to see if the name of the link has changed"
        )

    return int(table_df["content_length"].sum())


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def download_table(
    search_sicor_links_result: pd.DataFrame,
    table_id: str,
    download_all_files: bool = False,
) -> str:
    """Baixa e transforma os arquivos da tabela `table_id`. Retorna o diretório."""
    download_dir = create_folder_structure(table_id)

    table_df = filter_sicor_links(
        search_sicor_links_result,
        table_id,
        download_all_files=download_all_files,
    )

    if table_id == "empreendimento":
        create_empreendimento(table_id, download_dir)
    elif table_id == "fonte_recurso":
        create_fonte_recurso(table_id, download_dir)
    else:
        create_tables(table_df, table_id, download_dir)

    return str(download_dir)


@task(retries=TASK_RETRIES, retry_delay_seconds=TASK_RETRY_DELAY_SECONDS)
def create_load_dictionary() -> str:
    """Gera o CSV do dicionário e retorna o diretório."""
    return create_dictionary()


def _sicor_coverage(
    coverage_type: str, historical_database: bool
) -> CoverageSpec:
    """Mapeia (coverage_type, historical_database) runtime → CoverageSpec.

    Os callers usam apenas: part_bdpro+historical (default, colunas
    ano_emissao/mes_emissao mensal) e all_free+não-historical (empreendimento,
    cobertura única via metadata do BQ ⇒ NonHistorical)."""
    if not historical_database:
        return NonHistorical()
    date_column = YearMonth(year="ano_emissao", month="mes_emissao")
    if coverage_type == "part_bdpro":
        return PartBdpro(
            date_column=date_column, date_format=DateFormat.YEAR_MONTH
        )
    if coverage_type == "all_bdpro":
        return AllBdpro(
            date_column=date_column, date_format=DateFormat.YEAR_MONTH
        )
    if coverage_type == "all_free":
        return AllFree(
            date_column=date_column, date_format=DateFormat.YEAR_MONTH
        )
    raise ValueError(f"coverage_type não suportado: {coverage_type}")


def _run_bcb_sicor(
    dataset_id: str,
    table_id: str,
    materialize_after_dump: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
    dump_mode: str,
    source_format: str,
    coverage_type: str,
    historical_database: bool,
    download_all_files: bool = False,
    local_redis_execution: bool = False,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    download_links = search_sicor_links()

    table_size = get_sicor_table_size(
        download_links,
        table_id=table_id,
        download_all_files=download_all_files,
    )

    if not force_run:
        has_new_data = poll_source_size_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            byte_length=table_size,
            env="prod",
            local_execution=local_redis_execution,
        )
        if not has_new_data:
            print(f"Não há atualizações para a tabela {table_id}!")
            return

    print("Existem atualizações! A run será iniciada.")
    download_dir = download_table(
        download_links,
        table_id=table_id,
        download_all_files=download_all_files,
    )

    upload_to_gcs(
        data_path=download_dir,
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
        data_path=download_dir,
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
            coverage=_sicor_coverage(coverage_type, historical_database),
            env="prod",
            bq_project="basedosdados",
        )

        commit_source_size_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            byte_length=table_size,
            env="prod",
            local_execution=local_redis_execution,
        )
