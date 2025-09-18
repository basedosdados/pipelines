# -*- coding: utf-8 -*-
"""
Flow definition for the delete_flows pipeline
"""

from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.test_cd.tasks import logs_stuff
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import (
    constants as dump_db_constants,
)
from pipelines.utils.tasks import (
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
)

with Flow(
    name="Testar CD", code_owners=["guialvesp1"]
) as teste_cd_rumo_prefect_2:
    # Parameters
    dataset_id = Parameter("dataset_id", default="test_dataset", required=True)
    table_id = Parameter("table_id", default="test_table_ci_cd", required=True)

    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    target = Parameter("target", default="dev", required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    # Get the Prefect client
    test = logs_stuff(string="Hola Amigo")

    # Delete the old flow runs

    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels()
        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_STAGING_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id,
                "target": target,
                "dbt_alias": dbt_alias,
                "dbt_command": "run/test",
                "disable_elementary": False,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id}",
            upstream_tasks=[test],
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

teste_cd_rumo_prefect_2.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
teste_cd_rumo_prefect_2.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.BASEDOSDADOS_DEV_AGENT_LABEL.value],
)
