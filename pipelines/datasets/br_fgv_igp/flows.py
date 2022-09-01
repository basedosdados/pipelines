# -*- coding: utf-8 -*-
"""
Flows for br_fgv_igp
"""

from datetime import datetime, timedelta
from pathlib import Path

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.br_fgv_igp.schedules import igp_di_mes
from pipelines.datasets.br_fgv_igp.tasks import crawler_fgv, clean_fgv_df
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.tasks import create_table_and_upload_to_gcs
from pipelines.utils.tasks import (
    update_metadata,
    get_temporal_coverage,
    rename_current_flow_run_dataset_table,
    get_current_flow_labels,
)

ROOT = Path("tmp/data")

with Flow(
    name="IGP-DI mensal",
    code_owners=[],
) as fgv_igp_flow:
    # Parameters
    INDICE = Parameter("indice", default="IGP12_IGPDI12")
    PERIODO = Parameter("periodo", default="mes", required=False)
    dataset_id = Parameter("dataset_id", default="br_fgv_igp")
    table_id = Parameter("table_id")
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )

    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    df_indice = crawler_fgv(INDICE)
    filepath = clean_fgv_df(
        df_indice,
        root=ROOT,
        period=PERIODO,
        upstream_tasks=[
            df_indice,
        ],
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

    temporal_coverage = get_temporal_coverage(
        filepath=filepath,
        date_cols=["ano", "mes"],
        time_unit="month",
        interval="1",
        upstream_tasks=[filepath],
    )

    wait_update_metadata = update_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        fields_to_update=[
            {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}
        ],
        upstream_tasks=[filepath],
    )


fgv_igp_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
fgv_igp_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
fgv_igp_flow.schedule = igp_di_mes
