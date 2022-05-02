# -*- coding: utf-8 -*-
"""
DBT-related flows.
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect import Flow

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.execute_dbt_model.tasks import (
    get_k8s_dbt_client,
    run_dbt_model,
)
from pipelines.utils.tasks import rename_current_flow_run_dataset_table

with Flow(name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value) as run_dbt_model_flow:

    # Parameters
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    mode = Parameter("mode", default="dev", required=False)

    #################   ####################
    #
    # Rename flow run
    #
    #####################################
    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Materialize: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    # Get DBT client
    dbt_client = get_k8s_dbt_client(mode=mode, wait=rename_flow_run)

    # Run DBT model
    materialize_this = run_dbt_model(  # pylint: disable=invalid-name
        dbt_client=dbt_client,
        dataset_id=dataset_id,
        table_id=table_id,
        sync=True,
    )

run_dbt_model_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_dbt_model_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
