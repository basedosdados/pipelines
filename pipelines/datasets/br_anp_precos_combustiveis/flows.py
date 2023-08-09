# -*- coding: utf-8 -*-
"""
Flows for br_anp_precos_combustiveis
"""

from datetime import timedelta
from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from pipelines.constants import constants
from pipelines.utils.tasks import update_django_metadata
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.datasets.br_anp_precos_combustiveis.tasks import (
    tratamento,
    data_max_bd_mais,
    data_max_bd_pro,
)
from pipelines.datasets.br_anp_precos_combustiveis.schedules import (
    every_week_anp_microdados,
)
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    rename_current_flow_run_dataset_table,
    get_current_flow_labels,
)

with Flow(
    name="br_anp_precos_combustiveis.microdados", code_owners=["trick"]
) as anp_microdados:
    dataset_id = Parameter(
        "dataset_id", default="br_anp_precos_combustiveis", required=True
    )
    table_id = Parameter("table_id", default="microdados", required=True)

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
    update_metadata = Parameter("update_metadata", default=True, required=False)

    filepath = tratamento(upstream_tasks=[rename_flow_run])

    # pylint: disable=C0103
    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
    )

    # ! BD MAIS - Atrasado
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
        df = tratamento()
        with case(update_metadata, True):
            date = data_max_bd_mais(df=df)  # task que retorna a data atual
            update_django_metadata(
                dataset_id,
                table_id,
                metadata_type="DateTimeRange",
                bq_last_update=False,
                api_mode="prod",
                date_format="yy-mm-dd",
                _last_date=date,
                upstream_tasks=[df],
            )

    # ! BD PRO - Atualizado
    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels()
        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id + "_atualizado",
                "mode": materialization_mode,
                "dbt_alias": dbt_alias,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id}" "_atualizado",
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
            date = data_max_bd_pro(df=df)  # task que retorna a data atual
            update_django_metadata(
                dataset_id,
                table_id + "_atualizado",
                metadata_type="DateTimeRange",
                bq_last_update=False,
                api_mode="prod",
                date_format="yy-mm-dd",
                _last_date=date,
                upstream_tasks=[df],
            )

anp_microdados.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
anp_microdados.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
anp_microdados.schedule = every_week_anp_microdados
