# -*- coding: utf-8 -*-
"""
Flows for br_ms_cnes
"""
# pylint: disable=invalid-name

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.crawler.datasus.tasks import (
    access_ftp_download_files_async,
    check_files_to_parse,
    decompress_dbc,
    decompress_dbf,
    get_last_modified_date_in_sinan_tablen,
    is_empty,
    list_datasus_table_without_date,
    pre_process_files,
    read_dbf_save_parquet_chunks,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.flows import update_django_metadata
from pipelines.utils.metadata.tasks import check_if_data_is_outdated
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    download_data_to_gcs,
    log_task,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

# Disable Schedule: br_ms_cnes.profissional
with Flow(name="DATASUS-CNES", code_owners=["Gabriel Pisa"]) as flow_cnes:
    # Parameters
    dataset_id = Parameter("dataset_id", required=True)
    table_id = Parameter("table_id", required=True)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    year_month_to_extract = Parameter(
        "year_month_to_extract", default="", required=False
    )

    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    ftp_files = check_files_to_parse(
        dataset_id=dataset_id,
        table_id=table_id,
        year_month_to_extract=year_month_to_extract,
    )

    with case(is_empty(ftp_files), True):
        log_task(
            "Os dados do FTP CNES ainda não foram atualizados para o ano/mes mais recente"
        )

    with case(is_empty(ftp_files), False):
        dbc_files = access_ftp_download_files_async(
            file_list=ftp_files,
            dataset_id=dataset_id,
            table_id=table_id,
        )

        dbf_files = decompress_dbc(
            file_list=dbc_files,
            dataset_id=dataset_id,
            upstream_tasks=[dbc_files],
        )

        csv_files = decompress_dbf(
            file_list=dbc_files,
            table_id=table_id,
            upstream_tasks=[dbf_files, dbc_files],
        )

        files_path = pre_process_files(
            file_list=csv_files,
            dataset_id=dataset_id,
            table_id=table_id,
            upstream_tasks=[csv_files, dbf_files, dbc_files],
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=files_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=files_path,
        )

        # estabelecimento
        with case(materialize_after_dump, True):
            wait_for_materialization = run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                target=target,
                dbt_alias=dbt_alias,
                disable_elementary=False,
                upstream_tasks=[wait_upload_table],
            )
            wait_for_dowload_data_to_gcs = download_data_to_gcs(
                dataset_id=dataset_id,
                table_id=table_id,
                upstream_tasks=[wait_for_materialization],
            )

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_dowload_data_to_gcs],
                )
flow_cnes.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_cnes.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)


with Flow(name="DATASUS-SIA", code_owners=["Gabriel Pisa"]) as flow_siasus:
    # Parameters
    dataset_id = Parameter("dataset_id", required=True)
    table_id = Parameter("table_id", required=True)
    year_first_two_digits = Parameter("year_first_two_digits", required=False)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    year_month_to_extract = Parameter(
        "year_month_to_extract", default="", required=False
    )
    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    ftp_files = check_files_to_parse(
        dataset_id=dataset_id,
        table_id=table_id,
        year_month_to_extract=year_month_to_extract,
    )

    with case(is_empty(ftp_files), True):
        log_task(
            "Os dados do FTP SIA ainda não foram atualizados para o ano/mes mais recente"
        )

    with case(is_empty(ftp_files), False):
        dbc_files = access_ftp_download_files_async(
            file_list=ftp_files,
            dataset_id=dataset_id,
            table_id=table_id,
        )

        dbf_files = decompress_dbc(
            file_list=dbc_files,
            dataset_id=dataset_id,
            upstream_tasks=[dbc_files],
        )

        files_path = read_dbf_save_parquet_chunks(
            file_list=dbc_files,
            table_id=table_id,
            upstream_tasks=[dbc_files, dbf_files],
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=files_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=files_path,
        )

        with case(materialize_after_dump, True):
            wait_for_materialization = run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                target=target,
                dbt_alias=dbt_alias,
                dbt_command="run/test",
                disable_elementary=False,
                upstream_tasks=[wait_upload_table],
            )

            wait_for_dowload_data_to_gcs = download_data_to_gcs(
                dataset_id=dataset_id,
                table_id=table_id,
                upstream_tasks=[wait_for_materialization],
            )

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_dowload_data_to_gcs],
                )
flow_siasus.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_siasus.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    memory_limit="12Gi",
    memory_request="4Gi",
    cpu_limit=1,
)


with Flow(name="DATASUS-SIH", code_owners=["equipe_pipelines"]) as flow_sihsus:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_ms_sih", required=False)
    table_id = Parameter("table_id", default="aihs_reduzidas", required=False)
    year_first_two_digits = Parameter("year_first_two_digits", required=False)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    year_month_to_extract = Parameter(
        "year_month_to_extract", default="", required=False
    )
    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    ftp_files = check_files_to_parse(
        dataset_id=dataset_id,
        table_id=table_id,
        year_month_to_extract=year_month_to_extract,
    )

    with case(is_empty(ftp_files), True):
        log_task(
            "Os  dados do FTP SIH ainda não foram atualizados para o ano/mes mais recente"
        )

    with case(is_empty(ftp_files), False):
        dbc_files = access_ftp_download_files_async(
            file_list=ftp_files,
            dataset_id=dataset_id,
            table_id=table_id,
        )

        dbf_files = decompress_dbc(
            file_list=dbc_files,
            dataset_id=dataset_id,
            upstream_tasks=[dbc_files],
        )

        files_path = read_dbf_save_parquet_chunks(
            file_list=dbc_files,
            table_id=table_id,
            dataset_id=dataset_id,
            upstream_tasks=[dbc_files, dbf_files],
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=files_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=files_path,
            source_format="parquet",
        )

        with case(materialize_after_dump, True):
            wait_for_materialization = run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                target=target,
                dbt_alias=dbt_alias,
                dbt_command="run/test",
                disable_elementary=False,
                upstream_tasks=[wait_upload_table],
            )

            wait_for_dowload_data_to_gcs = download_data_to_gcs(
                dataset_id=dataset_id,
                table_id=table_id,
                upstream_tasks=[wait_for_materialization],
            )

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_dowload_data_to_gcs],
                )
flow_sihsus.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_sihsus.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)


with Flow(name="DATASUS-SINAN", code_owners=["trick"]) as flow_sinan:
    dataset_id = Parameter("dataset_id", default="br_ms_sinan", required=True)
    table_id = Parameter(
        "table_id", default="microdados_dengue", required=True
    )
    update_metadata = Parameter(
        "update_metadata", default=True, required=False
    )

    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    data_source_max_date = get_last_modified_date_in_sinan_tablen(
        datasus_database="SINAN", datasus_database_table="DENGBR"
    )

    dados_desatualizados = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=data_source_max_date,
        date_format="%Y-%m-%d",
        upstream_tasks=[data_source_max_date],
    )

    with case(dados_desatualizados, True):
        ftp_files = list_datasus_table_without_date(
            dataset_id=dataset_id,
            table_id=table_id,
        )
        dbc_files = access_ftp_download_files_async(
            file_list=ftp_files,
            dataset_id=dataset_id,
            table_id=table_id,
            upstream_tasks=[ftp_files],
        )

        dbf_files = decompress_dbc(
            file_list=dbc_files,
            dataset_id=dataset_id,
            upstream_tasks=[dbc_files],
        )

        files_path = read_dbf_save_parquet_chunks(
            file_list=dbc_files,
            dataset_id=dataset_id,
            table_id=table_id,
            upstream_tasks=[dbf_files, dbc_files],
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=files_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=files_path,
            upstream_tasks=[files_path],
        )

        with case(materialize_after_dump, True):
            wait_for_materialization = run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                target=target,
                dbt_alias=dbt_alias,
                disable_elementary=False,
                upstream_tasks=[wait_upload_table],
            )

            wait_for_dowload_data_to_gcs = download_data_to_gcs(
                dataset_id=dataset_id,
                table_id=table_id,
                upstream_tasks=[wait_for_materialization],
            )

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"date": "data_notificacao"},
                    date_format="%Y-%m-%d",
                    coverage_type="all_free",
                    time_delta={"months": 6},
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_dowload_data_to_gcs],
                )

flow_sinan.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_sinan.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
