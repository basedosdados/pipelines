# -*- coding: utf-8 -*-
"""
Flows for dumping data directly from BigQuery to GCS.
"""
from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.dump_to_gcs.constants import constants as dump_to_gcs_constants
from pipelines.utils.dump_to_gcs.tasks import (
    download_data_to_gcs,
    get_project_id,
    trigger_cron_job,
    update_last_trigger,
)
from pipelines.utils.tasks import rename_current_flow_run_dataset_table

with Flow(
    name=utils_constants.FLOW_DUMP_TO_GCS_NAME.value,
    code_owners=["lucas_cr"],
) as dump_to_gcs_flow:
    project_id = Parameter("project_id", required=False)
    dataset_id = Parameter("dataset_id")  # dataset_id or dataset_id_staging
    table_id = Parameter("table_id")
    query = Parameter("query", required=False)
    jinja_query_params = Parameter("jinja_query_params", required=False)
    bd_project_mode = Parameter(
        "bd_project_mode", required=False, default="prod"
    )  # prod or staging
    billing_project_id = Parameter("billing_project_id", required=False)
    maximum_bytes_processed = Parameter(
        "maximum_bytes_processed",
        required=False,
        default=dump_to_gcs_constants.MAX_BYTES_PROCESSED_PER_TABLE.value,
    )
    desired_crontab = Parameter(
        "desired_crontab",
        required=False,
        default="0 0 * * *",
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump to GCS: ", dataset_id=dataset_id, table_id=table_id
    )

    project_id = get_project_id(project_id=project_id, bd_project_mode=bd_project_mode)

    trigger_download, execution_time = trigger_cron_job(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        cron_expression=desired_crontab,
    )

    with case(trigger_download, True):
        download_task = download_data_to_gcs(  # pylint: disable=C0103
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            query=query,
            jinja_query_params=jinja_query_params,
            bd_project_mode=bd_project_mode,
            billing_project_id=billing_project_id,
            maximum_bytes_processed=maximum_bytes_processed,
        )

        update_task = update_last_trigger(  # pylint: disable=C0103
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            execution_time=execution_time,
        )
        update_task.set_upstream(download_task)


dump_to_gcs_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_to_gcs_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
