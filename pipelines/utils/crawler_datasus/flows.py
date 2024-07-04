# -*- coding: utf-8 -*-
"""
Flows for br_ms_cnes
"""
# pylint: disable=invalid-name

from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.crawler_datasus.constants import constants as br_datasus_constants
from pipelines.utils.crawler_datasus.tasks import (
    access_ftp_download_files_async,
    check_files_to_parse,
    decompress_dbc,
    decompress_dbf,
    is_empty,
    pre_process_files,
    read_dbf_save_parquet_chunks,
    list_datasus_table_without_date,
    get_last_modified_date_in_sinan_tablen
)
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.metadata.tasks import check_if_data_is_outdated
from pipelines.utils.metadata.flows import update_django_metadata
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    log_task,
    rename_current_flow_run_dataset_table,
)


with Flow(name="DATASUS-CNES", code_owners=["Gabriel Pisa"]) as flow_cnes:
    # Parameters
    dataset_id = Parameter("dataset_id", required=True)
    table_id = Parameter("table_id", required=True)
    update_metadata = Parameter("update_metadata", default=False, required=False)
    year_month_to_extract = Parameter("year_month_to_extract",default='', required=False)

    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
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
            file_list=dbc_files, dataset_id=dataset_id, upstream_tasks=[dbc_files]
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
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
                    "dbt_command": "run",
                    "disable_elementary": True,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {dataset_id}.{table_id}",
            )

            wait_for_materialization = wait_for_flow_run(
                materialization_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )
            wait_for_materialization.max_retries = (
                dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
            )
            wait_for_materialization.retry_delay = timedelta(
                seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
            )

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )
flow_cnes.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_cnes.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)




with Flow(name="DATASUS-SIA", code_owners=["Gabriel Pisa"]) as flow_siasus:
    # Parameters
    dataset_id = Parameter("dataset_id", required=True)
    table_id = Parameter("table_id", required=True)
    year_first_two_digits = Parameter("year_first_two_digits", required=False)
    update_metadata = Parameter("update_metadata", default=False, required=False)
    year_month_to_extract = Parameter("year_month_to_extract",default='', required=False)
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
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
            file_list=dbc_files, dataset_id=dataset_id, upstream_tasks=[dbc_files]
        )


        files_path = read_dbf_save_parquet_chunks(
            file_list=dbc_files,
            table_id=table_id,
            upstream_tasks=[dbc_files,dbf_files],
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=files_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=files_path,
        )

        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
                    "dbt_command": "run/test",
                    "disable_elementary": False,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {dataset_id}.{table_id}",
            )

            wait_for_materialization = wait_for_flow_run(
                materialization_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )
            wait_for_materialization.max_retries = (
                dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
            )
            wait_for_materialization.retry_delay = timedelta(
                seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
            )

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )
flow_siasus.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_siasus.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)



with Flow(name="DATASUS-SIH", code_owners=["arthurfg"]) as flow_sihsus:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_ms_sih", required=False)
    table_id = Parameter("table_id", default = 'aihs_reduzidas', required=False)
    year_first_two_digits = Parameter("year_first_two_digits", required=False)
    update_metadata = Parameter("update_metadata", default=False, required=False)
    year_month_to_extract = Parameter("year_month_to_extract",default='', required=False)
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
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
            file_list=dbc_files, dataset_id=dataset_id, upstream_tasks=[dbc_files]
        )


        files_path = read_dbf_save_parquet_chunks(
            file_list=dbc_files,
            table_id=table_id,
            dataset_id=dataset_id,
            upstream_tasks=[dbc_files,dbf_files],
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=files_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=files_path,
            source_format='parquet',
        )

        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
                    "dbt_command": "run/test",
                    "disable_elementary": False,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {dataset_id}.{table_id}",
            )

            wait_for_materialization = wait_for_flow_run(
                materialization_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )
            wait_for_materialization.max_retries = (
                dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
            )
            wait_for_materialization.retry_delay = timedelta(
                seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
            )

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )
flow_sihsus.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_sihsus.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)



with Flow(name="DATASUS-SINAN", code_owners=["trick"]) as flow_sinan:
    # Parameters
    dataset_id = Parameter("dataset_id", default ="br_ms_sinan", required=True)
    table_id = Parameter("table_id", default="microdados_dengue", required=True)
    update_metadata = Parameter("update_metadata", default=True, required=False)

    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    data_source_max_date = get_last_modified_date_in_sinan_tablen(datasus_database='SINAN', datasus_database_table='DENGBR')

    dados_desatualizados = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=data_source_max_date,
        date_format="%Y-%m-%d",
        upstream_tasks=[data_source_max_date],
        date_type = 'last_update_date'
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
            upstream_tasks=[ftp_files]
        )

        dbf_files = decompress_dbc(
            file_list=dbc_files,
            dataset_id=dataset_id,
            upstream_tasks=[dbc_files]
        )

        files_path = read_dbf_save_parquet_chunks(
            file_list=dbc_files,
            dataset_id=dataset_id,
            table_id=table_id,
            chunk_size = 150000,
            upstream_tasks=[dbf_files, dbc_files],
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=files_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=files_path,
            upstream_tasks=[files_path]
        )

        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
                    "dbt_command": "run",
                    "disable_elementary": True,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {dataset_id}.{table_id}",
                upstream_tasks=[wait_upload_table]
            )

            wait_for_materialization = wait_for_flow_run(
                materialization_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )
            wait_for_materialization.max_retries = (
                dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
            )
            wait_for_materialization.retry_delay = timedelta(
                seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
            )

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={'date' : 'data_notificacao'},
                    date_format="%Y-%m-%d",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )

flow_sinan.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_sinan.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)