# -*- coding: utf-8 -*-
"""
DBT-related flows.
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.dump_to_gcs.tasks import download_data_to_gcs
from pipelines.utils.execute_dbt_model.tasks import (
    run_dbt_model,
)
from pipelines.utils.tasks import rename_current_flow_run_dataset_table

with Flow(
    name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value
) as run_dbt_model_flow:
    # Parameters
    dataset_id = Parameter("dataset_id", required=True)
    table_id = Parameter("table_id", default=None, required=False)
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    dbt_command = Parameter("dbt_command", default="run", required=False)
    flags = Parameter("flags", default=None, required=False)
    target = Parameter("target", default="prod", required=False)
    _vars = Parameter("_vars", default=None, required=False)
    disable_elementary = Parameter(
        "disable_elementary", default=True, required=False
    )
    download_csv_file = Parameter(
        "download_csv_file", default=True, required=False
    )

    #################   ####################
    #
    # Rename flow run
    #
    #####################################
    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Materialize: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    # Run DBT model
    materialize_this = run_dbt_model(  # pylint: disable=invalid-name
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_alias=dbt_alias,
        dbt_command=dbt_command,
        target=target,
        flags=flags,
        _vars=_vars,
        disable_elementary=disable_elementary,
    )

    with case(download_csv_file, True):
        download_data_to_gcs(
            dataset_id=dataset_id,
            table_id=table_id,
            bd_project_mode=target,
            upstream_tasks=[materialize_this],
        )


run_dbt_model_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_dbt_model_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
