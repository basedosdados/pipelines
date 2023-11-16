# -*- coding: utf-8 -*-
"""
Flows for br_bcb_taxa_cambio.taxa_cambio
"""

from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.br_bcb_taxa_cambio.schedules import (
    schedule_every_weekday_taxa_cambio,
)
from pipelines.datasets.br_bcb_taxa_cambio.tasks import (
    get_data_taxa_cambio,
    treat_data_taxa_cambio,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
)

with Flow(
    name="br_bcb_taxa_cambio.taxa_cambio",
    code_owners=[
        "lauris",
    ],
) as datasets_br_bcb_taxa_cambio_moeda_flow:
    dataset_id = Parameter("dataset_id", default="br_bcb_taxa_cambio", required=True)
    table_id = Parameter("table_id", default="taxa_cambio", required=True)
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)
    update_metadata = Parameter("update_metadata", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    input_filepath = get_data_taxa_cambio(
        table_id=table_id, upstream_tasks=[rename_flow_run]
    )

    file_info = treat_data_taxa_cambio(
        table_id=table_id, upstream_tasks=[input_filepath]
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=file_info["save_output_path"],
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=file_info,
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

        with case(update_metadata, True):
            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                date_column_name={"date": "data"},
                date_format="%Y-%m-%d",
                coverage_type="all_bdpro",
                prefect_mode=materialization_mode,
                bq_project="basedosdados",
                upstream_tasks=[wait_for_materialization],
            )


datasets_br_bcb_taxa_cambio_moeda_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
datasets_br_bcb_taxa_cambio_moeda_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
datasets_br_bcb_taxa_cambio_moeda_flow.schedule = schedule_every_weekday_taxa_cambio
