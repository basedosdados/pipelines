# Project: pipelines
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import rename_current_flow_run_dataset_table
from pipelines.utils.template_flows.tasks import (
    create_table_and_upload_to_gcs_teste,
)

with Flow(
    name="BD Template: Create table and upload to GCS",
) as create_uploado_table_gcs:
    dataset_id = Parameter("dataset_id", required=True)
    table_id = Parameter("table_id", default=None, required=False)
    data_path = Parameter("data_path", required=True)
    dump_mode = Parameter("dump_mode", default="append", required=False)
    bucket_name = Parameter("bucket_name", required=True)
    source_format = Parameter("source_format", default="CSV", required=False)
    wait = Parameter("wait", default=None, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Create table and upload to GCS: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    rename_flow_run = create_table_and_upload_to_gcs_teste(
        data_path=data_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        bucket_name=bucket_name,
        source_format=source_format,
        wait=wait,
        upstream_tasks=[rename_flow_run],
    )

create_uploado_table_gcs.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
create_uploado_table_gcs.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
