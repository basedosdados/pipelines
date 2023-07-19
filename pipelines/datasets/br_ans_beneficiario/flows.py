# -*- coding: utf-8 -*-
"""
Flows for br_ans_beneficiario
"""


from datetime import datetime, timedelta
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.datasets.br_ans_beneficiario.tasks import ans

# from pipelines.datasets.br_ans_beneficiario.schedules import every_two_weeks
from pipelines.utils.decorators import Flow
from prefect import Parameter, case
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    rename_current_flow_run_dataset_table,
    get_current_flow_labels,
)
from prefect.tasks.prefect import (
    create_flow_run,
    wait_for_flow_run,
)


with Flow(
    name="br_ans_beneficiario.teste_beneficiario",
    code_owners=[
        "arthurfg",
    ],
) as datasets_br_ans_beneficiario_flow:
    dataset_id = Parameter("dataset_id", default="br_ans_beneficiario", required=True)
    table_id = Parameter("table_id", default="teste_beneficiario", required=True)
    data_inicio = Parameter("data_inicio", default="2023-01-01", required=False)
    data_fim = Parameter("data_fim", default="2023-01-01", required=False)
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)
    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )
    filepath = ans(data_inicio, data_fim)
    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
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

datasets_br_ans_beneficiario_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
datasets_br_ans_beneficiario_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# flow.schedule = every_two_weeks
