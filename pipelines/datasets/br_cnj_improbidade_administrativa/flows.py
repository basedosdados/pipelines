# -*- coding: utf-8 -*-
import datetime

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.br_cnj_improbidade_administrativa.schedules import every_month
from pipelines.datasets.br_cnj_improbidade_administrativa.tasks import (
    get_max_date,
    is_up_to_date,
    main_task,
    write_csv_file,
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
from pipelines.utils.utils import log_task

with Flow(
    name="br_cnj_improbidade_administrativa.condenacao",
    code_owners=[
        "aspeddro",
    ],
) as br_cnj_improbidade_administrativa_flow:
    dataset_id = Parameter("dataset_id", default="br_cnj_improbidade_administrativa", required=True)
    table_id = Parameter("table_id", default="condenacao", required=True)
    update_metadata = Parameter("update_metadata", default=True, required=False)
    materialization_mode = Parameter("materialization_mode", default="prod", required=False)
    materialize_after_dump = Parameter("materialize after dump", default=True, required=False)
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    is_updated = is_up_to_date()

    with case(is_updated, True):
        log_task("Data already updated")

    with case(is_updated, False):
        log_task("Data is outdated")

        df = main_task(upstream_tasks=[is_updated])

        log_task(df)

        max_date = get_max_date(df, upstream_tasks=[df])

        log_task(f"Max date: {max_date}")

        output_filepath = write_csv_file(df, upstream_tasks=[max_date])

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=output_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="overwrite",
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
                    "dbt_command": "run/test",
                },
                labels=current_flow_labels,
                run_name=r"Materialize {dataset_id}.{table_id}",
                upstream_tasks=[current_flow_labels],
            )

            wait_for_materialization = wait_for_flow_run(
                materialization_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
                upstream_tasks=[wait_upload_table],
            )
            wait_for_materialization.max_retries = (
                dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
            )
            wait_for_materialization.retry_delay = datetime.timedelta(
                seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
            )

        with case(update_metadata, True):
            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                date_column_name={"date": "data_propositura"},
                date_format="%Y-%m-%d",
                coverage_type="part_bdpro",
                prefect_mode=materialization_mode,
                time_delta={"months": 6},
                bq_project="basedosdados",
                upstream_tasks=[wait_for_materialization],
            )

br_cnj_improbidade_administrativa_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cnj_improbidade_administrativa_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_cnj_improbidade_administrativa_flow.schedule = every_month
