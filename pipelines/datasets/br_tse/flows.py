# -*- coding: utf-8 -*-
"""
Flows for br_tse
"""

from datetime import timedelta

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect import Parameter, case
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.constants import constants
from pipelines.datasets.br_tse.tasks import crawler_votacao_secao
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    rename_current_flow_run_dataset_table,
    get_current_flow_labels,
)
from pipelines.datasets.br_tse.schedules import every_monday_thursday


# pylint: disable=C0103
with Flow(
    name="br_tse.detalhes_votacao_secao", code_owners=["lucas_cr"]
) as votacao_secao_flow:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_tse", required=True)
    table_id = Parameter("table_id", default="detalhes_votacao_secao", required=True)
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize after dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)
    ufs = Parameter("ufs", default=["AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","SC","SE","RS","TO","SP"], required=False)
    anos = Parameter("anos", default=[2020], required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    filepath = crawler_votacao_secao(
        anos=anos,
        ufs=ufs
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
        wait=filepath,
    )

    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels(upstream_tasks=[wait_upload_table])
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
            upstream_tasks=[current_flow_labels]
        )

        wait_for_materialization = wait_for_flow_run(
            materialization_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
            upstream_tasks=[materialization_flow]
        )
        wait_for_materialization.max_retries = (
            dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
        )
        wait_for_materialization.retry_delay = timedelta(
            seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
        )

votacao_secao_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
votacao_secao_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
votacao_secao_flow.schedule = every_monday_thursday
