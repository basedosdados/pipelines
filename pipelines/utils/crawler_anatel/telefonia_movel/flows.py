# -*- coding: utf-8 -*-
# register flow
from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from datetime import timedelta
from pipelines.constants import constants
from pipelines.utils.crawler_anatel.telefonia_movel.tasks import (
    join_tables_in_function,
    get_max_date_in_table_microdados
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.metadata.tasks import (
    update_django_metadata,
    check_if_data_is_outdated
)
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
)

with Flow(
    name="BD template - Anatel Telefonia Móvel", code_owners=["trick"]
) as flow_anatel_telefonia_movel:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_anatel_telefonia_movel", required=True
    )
    table_id = Parameter(
        "table_id",
        required=True,
    )
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    ano = Parameter("ano", default=2024, required=False)

    semestre = Parameter("semestre", default=1, required=False)

    update_metadata = Parameter("update_metadata", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    update_tables = get_max_date_in_table_microdados(ano=ano, semestre=semestre)

    get_max_date = check_if_data_is_outdated(
    dataset_id =  dataset_id,
    table_id =  table_id,
    data_source_max_date = update_tables,
    date_format =  "%Y-%m",
)

    with case(get_max_date, True):

        filepath = join_tables_in_function(
            table_id = table_id,
            ano=ano,
            semestre=semestre,
            upstream_tasks=[get_max_date]
            )

        wait_upload_table = create_table_and_upload_to_gcs(
                data_path=filepath,
                dataset_id=dataset_id,
                table_id=table_id,
                dump_mode="append",
                wait=filepath,
                upstream_tasks=[filepath],  # Fix: Wrap filepath in a list to make it iterable
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
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )

flow_anatel_telefonia_movel.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_anatel_telefonia_movel.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)