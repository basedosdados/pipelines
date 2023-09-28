# -*- coding: utf-8 -*-
"""
Flows for dataset br_b3_cotacoes dataset
"""

from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.br_b3_cotacoes.schedules import all_day_cotacoes
from pipelines.datasets.br_b3_cotacoes.tasks import data_max_b3, tratamento
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
)
from pipelines.utils.utils import log

with Flow(name="br_b3_cotacoes.cotacoes", code_owners=["trick"]) as cotacoes:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_b3_cotacoes", required=True)
    table_id = Parameter("table_id", default="cotacoes", required=True)

    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    update_metadata = Parameter("update_metadata", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )
    delta_day = Parameter("delta_day", default=1, required=False)

    output_path = tratamento(delta_day=delta_day, upstream_tasks=[rename_flow_run])
    data_max = data_max_b3(delta_day=delta_day, upstream_tasks=[output_path])

    # pylint: disable=C0103
    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=output_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=output_path,
        upstream_tasks=[output_path],
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
        data_max = data_max_b3(
            delta_day=delta_day, upstream_tasks=[wait_for_materialization]
        )
        with case(update_metadata, True):  # task que retorna a data atual
            update_django_metadata(
                dataset_id,
                table_id,
                metadata_type="DateTimeRange",
                bq_last_update=False,
                bq_table_last_year_month=False,
                is_bd_pro=True,
                is_free=False,
                api_mode="prod",
                date_format="yy-mm-dd",
                _last_date=data_max,
                upstream_tasks=[wait_for_materialization],
            )

cotacoes.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cotacoes.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
cotacoes.schedule = all_day_cotacoes
