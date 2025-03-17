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
    download_repository,
    get_k8s_dbt_client,
    install_dbt_dependencies,
    new_execute_dbt_model,
    run_dbt_model,
)
from pipelines.utils.tasks import rename_current_flow_run_dataset_table

with Flow(
    name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value
) as run_dbt_model_flow:
    # Parameters
    dataset_id = Parameter("dataset_id", required=True)
    table_id = Parameter("table_id", default=None, required=False)
    mode = Parameter("mode", default="dev", required=False)
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    dbt_command = Parameter("dbt_command", default="run", required=False)
    flags = Parameter("flags", default=None, required=False)
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

    # Get DBT client
    dbt_client = get_k8s_dbt_client(mode=mode, wait=rename_flow_run)

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
        disable_elementary=disable_elementary,
    )

    with case(download_csv_file, True):
        download_data_to_gcs(
            dataset_id=dataset_id,
            table_id=table_id,
            bd_project_mode=mode,
            upstream_tasks=[materialize_this],
        )


run_dbt_model_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_dbt_model_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)


# The new flow using dbt CLI directly instead of RPC
with Flow(name="new_execute_dbt_model") as new_run_dbt_model_flow:
    # Parameters - keep the same parameter interface as the original flow
    dataset_id = Parameter("dataset_id", required=True)
    table_id = Parameter("table_id", default=None, required=False)
    mode = Parameter("mode", default="dev", required=False)
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    dbt_command = Parameter("dbt_command", default="run", required=True)
    flags = Parameter("flags", default=None, required=False)
    _vars = Parameter("_vars", default=None, required=False)
    disable_elementary = Parameter(
        "disable_elementary", default=True, required=False
    )
    target = Parameter("target", default="dev", required=False)
    download_csv_file = Parameter(
        "download_csv_file", default=True, required=False
    )
    generate_report = Parameter(
        "generate_report", default=True, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Materialize (CLI): ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    repository_path = download_repository()

    dependencies_installed = install_dbt_dependencies(
        dbt_repository_path=repository_path, upstream_tasks=[repository_path]
    )

    materialize_result = new_execute_dbt_model(
        dbt_repository_path=repository_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_alias=dbt_alias,
        sync=True,
        dbt_command=dbt_command,
        target=target,
        flags=flags,
        _vars=_vars,
        disable_elementary=disable_elementary,
        generate_report=generate_report,
        upstream_tasks=[dependencies_installed],
    )

    with case(download_csv_file, True):
        download_data_to_gcs(
            dataset_id=dataset_id,
            table_id=table_id,
            bd_project_mode=mode,
            upstream_tasks=[materialize_result, generate_report],
        )

new_run_dbt_model_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
new_run_dbt_model_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
