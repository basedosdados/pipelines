# -*- coding: utf-8 -*-

# register flow in prefect

from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from pipelines.constants import constants
from pipelines.utils.crawler_camara_dados_abertos.tasks import (
    save_data,
    check_if_url_is_valid,
    dict_update_django_metadata
)
from pipelines.utils.crawler_camara_dados_abertos.constants import constants as constants_camara
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    log_task,
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
)

# ------------------------------ TABLES UNIVERSAL -------------------------------------

with Flow(
    name="BD template - Camara Dados Abertos", code_owners=["trick"]
) as flow_camara_dados_abertos:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_camara_dados_abertos", required=True
    )
    table_id = Parameter(
        "table_id",
        required=True,
    )
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
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

    update_metadata = Parameter("update_metadata", default=False, required=False)

    with case(check_if_url_is_valid(table_id), True):
        # Save data to GCS
        filepath = save_data(
            table_id=table_id,
            upstream_tasks=[rename_flow_run],
        )
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=filepath,
            upstream_tasks=[filepath],  # Fix: Wrap filepath in a list to make it iterable
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

            get_table_id_in_dict_update_django_metadata = dict_update_django_metadata(table_id=table_id)
            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=get_table_id_in_dict_update_django_metadata["dataset_id"],
                    table_id=get_table_id_in_dict_update_django_metadata["table_id"],
                    date_column_name=get_table_id_in_dict_update_django_metadata['date_column_name'],
                    date_format=get_table_id_in_dict_update_django_metadata['date_format'],
                    coverage_type=get_table_id_in_dict_update_django_metadata["coverage_type"],
                    time_delta=get_table_id_in_dict_update_django_metadata['time_delta'],
                    prefect_mode=get_table_id_in_dict_update_django_metadata['prefect_mode'],
                    bq_project=get_table_id_in_dict_update_django_metadata["bq_project"],
                    upstream_tasks=[wait_for_materialization],
                )

flow_camara_dados_abertos.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_camara_dados_abertos.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)