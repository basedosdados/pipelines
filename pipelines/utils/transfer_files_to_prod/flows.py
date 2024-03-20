# -*- coding: utf-8 -*-
"""
Flows for transfer_files_to_prod
"""


from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.transfer_files_to_prod.tasks import download_files_from_bucket_folders
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants

from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
)


with Flow(
    name="utils.transfer_files_to_prod",
    code_owners=[
        "arthurfg",
    ],
) as transfer_files_to_prod:
    dataset_id = Parameter("dataset_id", default="br_cgu_beneficios_cidadao", required=False)
    table_id = Parameter("table_id", default="novo_bolsa_familia", required=False)
    folders = Parameter("folders", default=['mes_competencia=202306', 'mes_competencia=202305'], required=False)
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    output_filepath = download_files_from_bucket_folders(dataset_id =  dataset_id, table_id = table_id, folders = folders)
    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=output_filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=output_filepath,
    )
    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels(upstream_tasks=[wait_upload_table])
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
            upstream_tasks=[current_flow_labels]
        )

        wait_for_materialization = wait_for_flow_run(
            materialization_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
            upstream_tasks=[materialization_flow]
        )
        wait_for_materialization.max_retries = (
            dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
        )
        wait_for_materialization.retry_delay = timedelta(
            seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
        )


transfer_files_to_prod.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
transfer_files_to_prod.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)

