# -*- coding: utf-8 -*-
"""
Flows for world_sofascore_competicoes_futebol
"""

# pylint: disable=invalid-name,line-too-long
from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import (
    constants as dump_db_constants,
)
from pipelines.utils.metadata.tasks import (
    update_django_metadata,
)
from pipelines.utils.tasks import (
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
)

with Flow(
    name="BD template - world_sofascore_competicoes_futebol",
    code_owners=["luiz"],
) as world_sofascore_competicoes_futebol:
    # Parameters

    dataset_id = Parameter(
        "dataset_id",
        default="world_sofascore_competicoes_futebol",
        required=True,
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

    # data_source_max_date, ready_data_path = (
    #     get_data_source_max_date_and_preparing_data(
    #         table_id=table_id, upstream_tasks=[rename_flow_run]
    #     )
    # )

    # outdated = check_if_data_is_outdated(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     data_source_max_date=data_source_max_date,
    #     upstream_tasks=[data_source_max_date],
    # )

    # with case(outdated, True):
    # wait_upload_table = create_table_and_upload_to_gcs(
    #     data_path=rename_current_flow_run_dataset_table,
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     dump_mode="append",
    #     upstream_tasks=[rename_current_flow_run_dataset_table],
    # )

    # materialize
    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels()
        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_STAGING_PROJECT.value,
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

        with case(update_metadata, True):
            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                date_column_name={"date": "data"},
                date_format="%Y-%m-%d",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=target,
                bq_project="basedosdados-dev",
                upstream_tasks=[wait_for_materialization],
            )

world_sofascore_competicoes_futebol.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
world_sofascore_competicoes_futebol.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
