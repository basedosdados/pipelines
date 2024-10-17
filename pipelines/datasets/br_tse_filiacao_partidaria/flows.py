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

from pipelines.datasets.br_tse_filiacao_partidaria.tasks import (
    collector_starter,
    collect,
    processing,
    get_data_source_max_date
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
)
from pipelines.datasets.br_tse_filiacao_partidaria.schedules import (
    schedule_microdados
)

from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)

with Flow(
    name="br_tse_filiacao_partidaria.microdados", code_owners=["luiz"]
) as br_tse_filiacao_partidaria_microdados:

    # Parameters

    dataset_id = Parameter("dataset_id", default="br_tse_filiacao_partidaria", required=True)

    table_id = Parameter("table_id", default="microdados", required=True)

    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    update_metadata = Parameter("update_metadata", default=False, required=False)

    # Inicio das tarefas

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    data_source_max_date = get_data_source_max_date(upstream_tasks=[rename_flow_run])

    # outdated = check_if_data_is_outdated(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     date_type="last_update_date",
    #     data_source_max_date=data_source_max_date,
    #     upstream_tasks=[data_source_max_date],
    # )

    outdated = True

    with case(outdated, True):

        collector = collector_starter(upstream_tasks=[data_source_max_date])

        collect_task = collect(collector=collector, upstream_tasks=[collector])

        ready_data_path = processing(collector=collector, upstream_tasks=[collect_task])

        wait_upload_table = create_table_and_upload_to_gcs(
        data_path=ready_data_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        source_format= "csv",
        upstream_tasks=[ready_data_path]
        )

      # materialize municipio_exportacao
        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels(upstream_tasks=[wait_upload_table])
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
                    "download_csv_file": False
                },
                labels=current_flow_labels,
                run_name=f"Materialize {dataset_id}.{table_id}",
                upstream_tasks=[current_flow_labels]

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
                    date_column_name={"date": "data_extracao"},
                    date_format="%Y",
                    prefect_mode=materialization_mode,
                    coverage_type="all_free",
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )

br_tse_filiacao_partidaria_microdados.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_tse_filiacao_partidaria_microdados.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_tse_filiacao_partidaria_microdados.schedule = schedule_microdados