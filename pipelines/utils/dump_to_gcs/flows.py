# -*- coding: utf-8 -*-
"""
Flows for dumping data directly from BigQuery to GCS.
"""
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.dump_to_gcs.constants import constants as dump_to_gcs_constants
from pipelines.utils.dump_to_gcs.tasks import download_data_to_gcs

with Flow(
    name=utils_constants.FLOW_DUMP_TO_GCS_NAME.value,
    code_owners=[
        "equipe_infra",
        "equipe_dados",
    ],
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

    download_data_to_gcs(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        query=query,
        jinja_query_params=jinja_query_params,
        bd_project_mode=bd_project_mode,
        billing_project_id=billing_project_id,
        maximum_bytes_processed=maximum_bytes_processed,
    )

dump_to_gcs_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_to_gcs_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
