# -*- coding: utf-8 -*-
"""
Flows for mundo_transfermarkt_competicoes_internacionais
"""


from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.mundo_transfermarkt_competicoes_internacionais.schedules import (
    every_first_and_last_week,
)
from pipelines.datasets.mundo_transfermarkt_competicoes_internacionais.tasks import (
    execucao_coleta_sync,
    get_data_source_transfermarkt_max_date,
    make_partitions,
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

###############################################################################


with Flow(
    name="mundo_transfermarkt_competicoes_internacionais.champions_league",
    code_owners=[
        "Gabs",
    ],
) as transfermarkt_flow:
    dataset_id = Parameter(
        "dataset_id",
        default="mundo_transfermarkt_competicoes_internacionais",
        required=False,
    )
    table_id = Parameter("table_id", default="champions_league", required=False)
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    # rename_flow_run = rename_current_flow_run_dataset_table(
    #     prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    # )
    update_metadata = Parameter("update_metadata", default=False, required=False)

    data_source_max_date = get_data_source_transfermarkt_max_date()

    dados_desatualizados = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=data_source_max_date,
        date_format="%Y-%m-%d",
        upstream_tasks=[data_source_max_date],
    )

    with case(dados_desatualizados, True):
        df = execucao_coleta_sync()
        output_filepath = make_partitions(df, upstream_tasks=[df])

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
                    "dbt_command": "run/test",
                    "disable_elementary": False,
                },
                labels=current_flow_labels,
                run_name=r"Materialize {dataset_id}.{table_id}",
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

            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                date_column_name={"date": "data"},
                date_format="%Y-%m-%d",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=materialization_mode,
                bq_project="basedosdados",
                upstream_tasks=[wait_for_materialization],
            )


transfermarkt_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
transfermarkt_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
transfermarkt_flow.schedule = every_first_and_last_week
