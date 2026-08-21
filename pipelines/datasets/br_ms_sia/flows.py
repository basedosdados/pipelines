"""
Flows for br_ms_sia — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.datasus.tasks import (
    access_ftp_download_files_async,
    check_files_to_parse,
    decompress_dbc,
    get_datasus_source_max_date,
    read_dbf_save_parquet_chunks,
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


def _sia_flow(table_id: str, cron: str):
    @flow(
        name=f"br_ms_sia__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_ms_sia",
        table_id: str = table_id,
        materialize_after_dump: bool = True,
        dbt_alias: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
        year_month_to_extract: str = "",
        source_format: str = "parquet",
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
                compare_against="coverage",
            )
            if not has_new_data:
                print("Fonte SIA sem novidade — encerrando")
                return

        # Comita o Update da fonte já aqui, antes de baixar/materializar: se o
        # flow falhar no meio, o metadado da fonte ainda reflete que havia dado
        # novo publicado, mesmo que a tabela não tenha sido atualizada.
        commit_source_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=source_max_date,
            env="prod",
            date_format="%Y-%m",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_after_dump,
        )

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

        # if not materialize_after_dump:
        #     return

        # upload_to_gcs(
        #     data_path=files_path,
        #     dataset_id=dataset_id,
        #     table_id=table_id,
        #     bucket_name="basedosdados",
        #     dump_mode="append",
        #     source_format=source_format,
        # )
        # run_dbt(
        #     dataset_id=dataset_id,
        #     table_id=table_id,
        #     dbt_command="run/test",
        #     dbt_alias=dbt_alias,
        #     target=target,
        # )

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

    # pyrefly: ignore [missing-attribute]
    _flow.deploy_schedules = [{"cron": cron, "timezone": "America/Sao_Paulo"}]
    return _flow


br_ms_sia__producao_ambulatorial = _sia_flow(
    "producao_ambulatorial", "0 21 * * *"
)
br_ms_sia__psicossocial = _sia_flow("psicossocial", "0 7 * * *")
