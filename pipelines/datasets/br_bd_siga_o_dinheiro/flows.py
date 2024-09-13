# -*- coding: utf-8 -*-
"""
Flows for br_jota
"""

from datetime import timedelta

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.tasks import get_current_flow_labels

from pipelines.datasets.br_bd_siga_o_dinheiro.tasks import (
    get_table_ids
)

from pipelines.datasets.br_bd_siga_o_dinheiro.schedules import (
    schedule_br_jota_2024
)

with Flow(
    name="BD template - br_bd_siga_o_dinheiro", code_owners=["luiz"]
) as br_jota_2024:

    dataset_id = Parameter("dataset_id", default="br_bd_siga_o_dinheiro", required=True)

    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    table_ids = get_table_ids()

    for n, table_id in enumerate(table_ids):

        current_flow_labels = get_current_flow_labels(upstream_tasks=[wait_for_materialization] if n > 0 else None)
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


br_jota_2024.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_jota_2024.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_jota_2024.schedule = schedule_br_jota_2024
