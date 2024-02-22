# -*- coding: utf-8 -*-
"""
DBT-related flows.
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.tasks import get_k8s_dbt_client, run_dbt_model
from pipelines.utils.tasks import rename_current_flow_run_dataset_table

with Flow(name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value) as run_dbt_model_flow:
    # Parameters
    dataset_id = Parameter("dataset_id", required=True)
    table_id = Parameter("table_id", default=None, required=False)
    mode = Parameter("mode", default="dev", required=False)
    dbt_alias = Parameter("dbt_alias", default=False, required=False)
    dbt_command = Parameter("dbt_command", default="run", required=False)
    flags = Parameter("flags", default=None, required=False)
    _vars = Parameter("_vars", default=None, required=False)
    disable_elementary = Parameter("disable_elementary", default=False, required=False)

    #################   ####################
    #
    # Rename flow run
    #
    #####################################
    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Materialize: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    # Get DBT client
    dbt_client = get_k8s_dbt_client(mode=mode,
    wait=rename_flow_run
    )

    # Run DBT model
    materialize_this = run_dbt_model(  # pylint: disable=invalid-name
        dbt_client=dbt_client,
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_alias=dbt_alias,
        sync=True,
        dbt_command=dbt_command,
        flags=flags,
        _vars=_vars,
        disable_elementary = disable_elementary
    )


run_dbt_model_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_dbt_model_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
