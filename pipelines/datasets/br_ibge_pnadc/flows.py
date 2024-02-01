# -*- coding: utf-8 -*-
"""
Flows for br_ibge_pnadc
"""

from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.br_ibge_pnadc.schedules import every_quarter
from pipelines.datasets.br_ibge_pnadc.tasks import (  # save_partitions,
    build_parquet_files,
    download_txt,
    get_url_from_template,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.metadata.flows import update_django_metadata
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
)
from pipelines.utils.to_download.tasks import to_download

# pylint: disable=C0103
with Flow(name="br_ibge_pnadc.microdados", code_owners=["lauris"]) as br_pnadc:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_ibge_pnadc", required=False)
    table_id = Parameter("table_id", default="microdados", required=False)
    year = Parameter("year", default=2022, required=False)
    quarter = Parameter("quarter", default=3, required=False)
    update_metadata = Parameter("update_metadata", default=False, required=False)
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    url = get_url_from_template(year, quarter, upstream_tasks=[rename_flow_run])

    input_filepath = to_download(url, "/tmp/data/input/", "zip")

    output_filepath = build_parquet_files(
        input_filepath, upstream_tasks=[input_filepath]
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=output_filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=output_filepath,
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
                date_column_name={"year": "ano", "quarter": "trimestre"},
                date_format="%Y-%m",
                coverage_type="all_free",
                prefect_mode=materialization_mode,
                bq_project="basedosdados",
                upstream_tasks=[wait_for_materialization],
            )

br_pnadc.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_pnadc.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_pnadc.schedule = every_quarter
