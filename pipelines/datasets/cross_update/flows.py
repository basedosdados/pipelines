# -*- coding: utf-8 -*-
"""
Flows for br_tse_eleicoes
"""
# pylint: disable=invalid-name,line-too-long

from datetime import timedelta

import prefect
from prefect import Parameter, case, unmapped
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.cross_update.tasks import (
    get_metadata_data,
    query_tables,
    update_metadata_and_filter,
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
    name="cross_update.update_nrows", code_owners=["lauris"]
) as crossupdate_nrows:
    dump_to_gcs = Parameter("dump_to_gcs", default=False, required=False)
    days = Parameter("days", default=7, required=False)
    mode = Parameter("mode", default="prod", required=False)

    update_metadata_table_flow = create_flow_run(
        flow_name="cross_update.update_metadata_table",
        project_name="staging",
        parameters={"materialization_mode": mode},
    )

    wait_for_create_table = wait_for_flow_run(
        update_metadata_table_flow,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )

    eligible_to_zip_tables = query_tables(
        days=days, mode=mode, upstream_tasks=[wait_for_create_table]
    )
    tables_to_zip = update_metadata_and_filter(
        eligible_to_zip_tables, upstream_tasks=[eligible_to_zip_tables]
    )

    with case(dump_to_gcs, True):
        current_flow_labels = get_current_flow_labels()
        dump_to_gcs_flow = create_flow_run.map(
            flow_name=unmapped(utils_constants.FLOW_DUMP_TO_GCS_NAME.value),
            project_name=unmapped("staging"),
            parameters=tables_to_zip,
            labels=unmapped(current_flow_labels),
            run_name=unmapped("Dump to GCS"),
        )

        wait_for_dump_to_gcs = wait_for_flow_run.map(
            dump_to_gcs_flow,
            stream_states=unmapped(True),
            stream_logs=unmapped(True),
            raise_final_state=unmapped(True),
        )

        # rename_blobs(upstream_tasks=[wait_for_dump_to_gcs])


crossupdate_nrows.storage = GCS(str(constants.GCS_FLOWS_BUCKET.value))
crossupdate_nrows.run_config = KubernetesRun(image=str(constants.DOCKER_IMAGE.value))
# crossupdate_nrows.schedule = schedule_nrows

with Flow(
    name="cross_update.update_metadata_table", code_owners=["lauris"]
) as crossupdate_update_metadata_table:
    dataset_id = Parameter("dataset_id", default="br_bd_metadados", required=False)
    table_id = Parameter("table_id", default="bigquery_tables", required=False)
    update_metadata = Parameter("update_metadata", default=False, required=False)
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    file_path = get_metadata_data(
        mode=materialization_mode, upstream_tasks=[materialization_mode]
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=file_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        upstream_tasks=[file_path],
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
            upstream_tasks=[wait_upload_table],
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
            coverage_type="all_free",
            prefect_mode=materialization_mode,
            bq_project="basedosdados",
            historical_database=False,
        )


crossupdate_update_metadata_table.storage = GCS(str(constants.GCS_FLOWS_BUCKET.value))
crossupdate_update_metadata_table.run_config = KubernetesRun(
    image=str(constants.DOCKER_IMAGE.value)
)
# crossupdate_nrows.schedule = schedule_nrows
