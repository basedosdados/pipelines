# -*- coding: utf-8 -*-
from datetime import timedelta
from itertools import product

from prefect import Parameter, case, unmapped
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants as pipelines_constants
from pipelines.datasets.br_denatran_frota.constants import constants
from pipelines.datasets.br_denatran_frota.tasks import (
    crawl_task,
    get_denatran_date,
    get_desired_file_task,
    get_latest_data_task,
    output_file_to_csv_task,
    should_process_data_task,
    treat_municipio_tipo_task,
    treat_uf_tipo_task,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
)

year_range = list(range(2003, 2023))
month_range = list(range(1, 13))
date_pairs = list(product(year_range, month_range))

date_pairs_param: list[tuple] = Parameter("date_pairs", default=date_pairs)

# from pipelines.datasets.br_denatran_frota.schedules import every_two_weeks

with Flow(
    name="br_denatran_frota.uf_tipo",
    code_owners=[
        "Tamir",
    ],
) as br_denatran_frota_uf_tipo:
    dataset_id = Parameter("dataset_id", default="br_denatran_frota")
    table_id = Parameter("table_id", default="uf_tipo")

    # Materialization mode
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )

    materialize_after_dump = Parameter(
        "materialize after dump", default=False, required=False
    )

    update_metadata = Parameter("update_metadata", default=False, required=False)

    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    year_to_fetch = get_latest_data_task(table_id="uf_tipo", dataset_id=dataset_id)
    crawled = crawl_task(
        month=year_to_fetch[1],
        year=year_to_fetch[0],
        temp_dir=constants.DOWNLOAD_PATH.value,
        upstream_tasks=[year_to_fetch],
    )
    # Now get the downloaded file:
    desired_file = get_desired_file_task(
        year=year_to_fetch[0],
        download_directory=constants.DOWNLOAD_PATH.value,
        filetype=constants.UF_TIPO_BASIC_FILENAME.value,
        upstream_tasks=[crawled],
    )
    # We need to see the year, month from the file to decide on updating
    extracted_date = get_denatran_date(desired_file, upstream_tasks=[desired_file])
    decision = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=extracted_date,
        date_format="%Y-%m",
        upstream_tasks=[extracted_date],
    )
    with case(decision, True):
        df = treat_uf_tipo_task(
            file=desired_file, upstream_tasks=[crawled, desired_file]
        )
        csv_output = output_file_to_csv_task(
            df, constants.UF_TIPO_BASIC_FILENAME.value, upstream_tasks=[df]
        )
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=csv_output,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=csv_output,
        )

        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=pipelines_constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
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
                    coverage_type="all_free",
                    # time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )


br_denatran_frota_uf_tipo.storage = GCS(pipelines_constants.GCS_FLOWS_BUCKET.value)
br_denatran_frota_uf_tipo.run_config = KubernetesRun(
    image=pipelines_constants.DOCKER_IMAGE.value
)
# flow.schedule = every_two_weeks


with Flow(
    name="br_denatran_frota.municipio_tipo",
    code_owners=[
        "Tamir",
    ],
) as br_denatran_frota_municipio_tipo:
    dataset_id = Parameter("dataset_id", default="br_denatran_frota")
    table_id = Parameter("table_id", default="municipio_tipo")

    # Materialization mode
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )

    materialize_after_dump = Parameter(
        "materialize after dump", default=False, required=False
    )

    update_metadata = Parameter("update_metadata", default=False, required=False)

    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    # inserir  get_api_most_recente_date
    # na função get_latest_data
    year_to_fetch = get_latest_data_task(
        table_id="municipio_tipo", dataset_id=dataset_id
    )

    crawled = crawl_task(
        month=year_to_fetch[1],
        year=year_to_fetch[0],
        temp_dir=constants.DOWNLOAD_PATH.value,
        upstream_tasks=[year_to_fetch],
    )
    # Now get the downloaded file:
    desired_file = get_desired_file_task(
        year=year_to_fetch[0],
        download_directory=constants.DOWNLOAD_PATH.value,
        filetype=constants.MUNIC_TIPO_BASIC_FILENAME.value,
        upstream_tasks=[crawled],
    )
    # We need to see the year, month from the file to decide on updating
    extracted_date = get_denatran_date(desired_file, upstream_tasks=[desired_file])

    decision = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=extracted_date,
        date_format="%Y-%m",
        upstream_tasks=[extracted_date],
    )
    with case(decision, True):
        df = treat_municipio_tipo_task(file=desired_file, upstream_tasks=[decision])
        csv_output = output_file_to_csv_task(
            df, constants.MUNIC_TIPO_BASIC_FILENAME.value, upstream_tasks=[df]
        )
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=csv_output,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=csv_output,
        )

        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=pipelines_constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
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
                    coverage_type="all_free",
                    # time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )


br_denatran_frota_municipio_tipo.storage = GCS(
    pipelines_constants.GCS_FLOWS_BUCKET.value
)
br_denatran_frota_municipio_tipo.run_config = KubernetesRun(
    image=pipelines_constants.DOCKER_IMAGE.value
)
# flow.schedule = every_two_weeks
