# -*- coding: utf-8 -*-
"""
Flows for br_rf_cno
"""
# pylint: disable=invalid-name
from datetime import timedelta

from prefect import Parameter, case, unmapped
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.br_rf_cno.constants import constants as br_rf_cno_constants
from pipelines.datasets.br_rf_cno.tasks import (
    crawl_cno,
    wrangling,
    )

from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    log_task,
    rename_current_flow_run_dataset_table,
)

with Flow(
    name="br_rf_cno.tables", code_owners=["Gabriel Pisa"]
) as br_rf_cno:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_rf_cno", required=True)
    table_id = Parameter("table_id", default="tables", required=False)
    table_ids = Parameter("table_ids", default=['microdados', 'areas', 'cnaes', 'vinculos'], required=False)
    update_metadata = Parameter("update_metadata", default=False, required=False)
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

    data = crawl_cno(
        root='input',
        url=br_rf_cno_constants.URL.value
        )

    files = wrangling(input_dir='input', output_dir='output',
        upstream_tasks=[data])

    #3. subir tabelas para o Storage e materilizar no BQ usando map
    wait_upload_table = create_table_and_upload_to_gcs.map(
        data_path=unmapped(f'output'),
        dataset_id=unmapped(dataset_id),
        table_id=table_ids,
        dump_mode=unmapped("append"),
        source_format='parquet',
        wait=files
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
                    date_column_name={"date": "date"},
                    date_format="%Y-%m-%d",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )


br_rf_cno.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_rf_cno.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
