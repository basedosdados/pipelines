# -*- coding: utf-8 -*-
from datetime import timedelta
from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from pipelines.constants import constants
from pipelines.datasets.br_cgu_emendas_parlamentares.schedules import (
    every_day_emendas_parlamentares
)
from pipelines.datasets.br_cgu_emendas_parlamentares.tasks import (
    convert_str_to_float,
    get_last_modified_time
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.metadata.tasks import (
    update_django_metadata,
    check_if_data_is_outdated,
)
from pipelines.utils.tasks import (  # update_django_metadata,
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
)


with Flow(
    name="br_cgu_emendas_parlamentares.microdados",
    code_owners=[
        "trick",
    ],
) as br_cgu_emendas_parlamentares_flow:
    dataset_id = Parameter("dataset_id", default="br_cgu_emendas_parlamentares", required=False)
    table_id = Parameter("table_id", default="microdados", required=False)
    update_metadata = Parameter("update_metadata", default=False, required=False)
    materialization_mode = Parameter("materialization_mode", default="dev", required=False)
    materialize_after_dump = Parameter("materialize_after_dump", default=False, required=False)
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    rename_flow_run = rename_current_flow_run_dataset_table(prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id)

    max_modified_time = get_last_modified_time()

    outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        date_type="last_update_date",
        data_source_max_date=max_modified_time,
        upstream_tasks=[max_modified_time],
    )

    with case(outdated, True):
        output_path = convert_str_to_float()
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=output_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=output_path,
            upstream_tasks=[output_path],
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
                upstream_tasks = [wait_upload_table]
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
                    date_format="%Y",
                    date_column_name = {"year": "ano_emenda"},
                    coverage_type="part_bdpro",
                    time_delta =  {"years": 1},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )

br_cgu_emendas_parlamentares_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_emendas_parlamentares_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_cgu_emendas_parlamentares_flow.schedule = every_day_emendas_parlamentares