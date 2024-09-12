# -*- coding: utf-8 -*-
from prefect import task
from datetime import timedelta


from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.utils import log


@task
def materialization(number_table, dataset_id, table_id, materialization_mode, dbt_alias, current_flow_labels):

    log("Estou rodando a tabela {number_table}")

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

    return wait_for_materialization