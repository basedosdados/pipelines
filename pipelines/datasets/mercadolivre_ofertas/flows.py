# -*- coding: utf-8 -*-
"""
Flows for mercadolivre_ofertas
"""
# pylint: disable=invalid-name
from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants

from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.tasks import (
    rename_current_flow_run_dataset_table,
    get_current_flow_labels,
    create_table_and_upload_to_gcs,
)

from pipelines.datasets.mercadolivre_ofertas.tasks import crawler_mercadolivre_ofertas
from pipelines.datasets.mercadolivre_ofertas.schedules import every_day_item

with Flow(
    name="mercadolivre_ofertas.item", code_owners=["lucascr91"]
) as mercadolivre_ofertas:
    # Parameters
    dataset_id = Parameter("dataset_id", default="mercadolivre_ofertas", required=True)
    table_id = Parameter("table_id", default="item", required=True)
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    filepath = crawler_mercadolivre_ofertas()

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
        wait=filepath,
    )

    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels()
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

mercadolivre_ofertas.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
mercadolivre_ofertas.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
mercadolivre_ofertas.schedule = every_day_item
