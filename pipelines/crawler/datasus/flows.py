"""
Shared run logic for DATASUS pipelines (CNES, SIA, SIH, SINAN) — Prefect 3.
"""

from pipelines.crawler.datasus.tasks import (
    access_ftp_download_files_async,
    check_files_to_parse,
    decompress_dbc,
    decompress_dbf,
    get_datasus_source_max_date,
    get_last_modified_date_in_sinan_tablen,
    list_datasus_table_without_date,
    pre_process_files,
    read_dbf_save_parquet_chunks,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    DateOnly,
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


def _run_cnes(
    dataset_id: str,
    table_id: str,
    materialize_after_dump: bool,
    dbt_alias: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
    year_month_to_extract: str = "",
) -> None:
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    ftp_files = check_files_to_parse(
        dataset_id=dataset_id,
        table_id=table_id,
        year_month_to_extract=year_month_to_extract,
    )
    source_max_date = get_datasus_source_max_date(ftp_files)

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=source_max_date,
            env="prod",
            date_format="%Y-%m",
        )
        if not has_new_data:
            print("Fonte CNES sem novidade — encerrando")
            return

    if not ftp_files:
        print("force_run=True mas FTP não retornou arquivos — encerrando")
        return

    dbc_files = access_ftp_download_files_async(
        file_list=ftp_files, dataset_id=dataset_id, table_id=table_id
    )
    decompress_dbc(file_list=dbc_files, dataset_id=dataset_id)
    csv_files = decompress_dbf(file_list=dbc_files, table_id=table_id)
    # pyrefly: ignore [no-matching-overload]
    files_path = pre_process_files(
        file_list=csv_files, dataset_id=dataset_id, table_id=table_id
    )

    upload_to_gcs(
        data_path=files_path,
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
        data_path=files_path,
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

        if source_max_date is not None:
            commit_source_update_task(
                dataset_id=dataset_id,
                table_id=table_id,
                source_max_date=source_max_date,
                env="prod",
                date_format="%Y-%m",
            )


def _run_dbf_to_parquet(
    dataset_id: str,
    table_id: str,
    materialize_after_dump: bool,
    dbt_alias: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
    source_format: str,
    fonte_label: str,
    year_month_to_extract: str = "",
) -> None:
    """Shared logic for SIA/SIH (DBF→Parquet pipeline)."""
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    ftp_files = check_files_to_parse(
        dataset_id=dataset_id,
        table_id=table_id,
        year_month_to_extract=year_month_to_extract,
    )
    source_max_date = get_datasus_source_max_date(ftp_files)

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=source_max_date,
            env="prod",
            date_format="%Y-%m",
        )
        if not has_new_data:
            print(f"Fonte {fonte_label} sem novidade — encerrando")
            return

    if not ftp_files:
        print("force_run=True mas FTP não retornou arquivos — encerrando")
        return

    dbc_files = access_ftp_download_files_async(
        file_list=ftp_files, dataset_id=dataset_id, table_id=table_id
    )
    decompress_dbc(file_list=dbc_files, dataset_id=dataset_id)
    files_path = read_dbf_save_parquet_chunks(
        file_list=dbc_files, table_id=table_id, dataset_id=dataset_id
    )

    upload_to_gcs(
        data_path=files_path,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="append",
        source_format=source_format,
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
        data_path=files_path,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados",
        dump_mode="append",
        source_format=source_format,
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

        if source_max_date is not None:
            commit_source_update_task(
                dataset_id=dataset_id,
                table_id=table_id,
                source_max_date=source_max_date,
                env="prod",
                date_format="%Y-%m",
            )


def _run_siasus(**kwargs) -> None:
    _run_dbf_to_parquet(source_format="csv", fonte_label="SIA", **kwargs)


def _run_sihsus(**kwargs) -> None:
    _run_dbf_to_parquet(source_format="parquet", fonte_label="SIH", **kwargs)


def _run_sinan(
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

    data_source_max_date = get_last_modified_date_in_sinan_tablen(
        datasus_database="SINAN", datasus_database_table="DENGBR"
    )

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=data_source_max_date,
            env="prod",
            date_format="%Y-%m-%d",
        )
        if not has_new_data:
            print("Sem atualizações na fonte SINAN — encerrando")
            return

    ftp_files = list_datasus_table_without_date(
        dataset_id=dataset_id, table_id=table_id
    )
    dbc_files = access_ftp_download_files_async(
        file_list=ftp_files, dataset_id=dataset_id, table_id=table_id
    )
    decompress_dbc(file_list=dbc_files, dataset_id=dataset_id)
    files_path = read_dbf_save_parquet_chunks(
        file_list=dbc_files, table_id=table_id, dataset_id=dataset_id
    )

    upload_to_gcs(
        data_path=files_path,
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
        data_path=files_path,
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
                date_column=DateOnly(col="data_notificacao"),
                date_format=DateFormat.YEAR_MD,
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
