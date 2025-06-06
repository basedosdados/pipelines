# -*- coding: utf-8 -*-
"""
Flows for br_tse_eleicoes
"""

# pylint: disable=invalid-name,line-too-long
from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.crawler_tse_eleicoes.tasks import (
    flows_control,
    get_data_source_max_date,
    preparing_data,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import (
    constants as dump_db_constants,
)
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
)

with Flow(
    name="BD template - BR_TSE_ELEICOES", code_owners=["luiz"]
) as br_tse_eleicoes:
    # Parameters

    dataset_id = Parameter(
        "dataset_id", default="br_tse_eleicoes", required=True
    )

    table_id = Parameter("table_id", required=True)

    target = Parameter("target", default="prod", required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )

    # Inicio das tarefas

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    flow = flows_control(
        table_id=table_id,
        mode=target,
        upstream_tasks=[rename_flow_run],
    )

    data_source_max_date = get_data_source_max_date(
        flow_class=flow, upstream_tasks=[flow]
    )

    outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        date_type="last_update_date",
        data_source_max_date=data_source_max_date,
        upstream_tasks=[data_source_max_date],
    )

    with case(outdated, True):
        ready_data_path = preparing_data(
            flow_class=flow, upstream_tasks=[outdated]
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=ready_data_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[ready_data_path],
        )

        # materialize municipio_exportacao
        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "target": target,
                    "dbt_alias": dbt_alias,
                    "dbt_command": "run/test",
                    "disable_elementary": False,
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
            # coverage updater

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"date": "data_eleicao"},
                    date_format="%Y",
                    prefect_mode=target,
                    coverage_type="all_free",
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )

br_tse_eleicoes.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_tse_eleicoes.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
