# -*- coding: utf-8 -*-
from datetime import timedelta

from prefect import Parameter, case
from prefect.tasks.prefect import (
    create_flow_run,
    wait_for_flow_run,
)

from pipelines.datasets.br_bcb_agencia.schedules import (
    every_month_agencia,
)
from pipelines.datasets.br_bcb_agencia.tasks import (
    download_data,
    clean_data,
)
from pipelines.datasets.br_bcb_agencia.constants import (
    constants as agencia_constants,
)

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.utils.decorators import Flow
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.metadata.flows import update_django_metadata
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    rename_current_flow_run_dataset_table,
    get_current_flow_labels,
)


with Flow(
    name="br_bcb_agencia.agencia",
    code_owners=[
        "Gabriel Pisa",
    ],
) as br_bcb_agencia_agencia:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_bcb_agencia", required=True)
    table_id = Parameter("table_id", default="agencia", required=True)
    update_metadata = Parameter("update_metadata", default=False, required=False)
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    # Materialization mode
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    temxpes = download_data(
        url=agencia_constants.AGENCIA_URL.value,
        xpath=agencia_constants.AGENCIA_XPATH.value,
    )

    filepath = clean_data(
        upstream_tasks=[temxpes],
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
    )

    # agencia
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
            update = update_django_metadata(
                dataset_id,
                table_id,
                metadata_type="DateTimeRange",
                bq_last_update=False,
                bq_table_last_year_month=True,
                api_mode="prod",
                billing_project_id="basedosdados",
                date_format="yy-mm",
                is_bd_pro=True,
                is_free=True,
                time_delta=6,
                time_unit="months",
                upstream_tasks=[wait_for_materialization],
            )


br_bcb_agencia_agencia.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_bcb_agencia_agencia.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_bcb_agencia_agencia.schedule = every_month_agencia
