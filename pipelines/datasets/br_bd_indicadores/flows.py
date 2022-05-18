# -*- coding: utf-8 -*-
"""
Flows for br_bd_indicadores
"""
from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.datasets.br_bd_indicadores.tasks import (
    crawler_metricas,
    has_new_tweets,
    echo,
    get_credentials,
)
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    rename_current_flow_run_dataset_table,
    get_current_flow_labels,
)

from pipelines.datasets.br_bd_indicadores.schedules import every_day, every_week

with Flow(name="br_bd_indicadores.twitter_metrics") as bd_twt_metricas:
    # Parameters
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )
    dataset_id = Parameter("dataset_id", default="br_bd_indicadores", required=True)
    table_id = Parameter("table_id", default="twitter_metrics", required=True)
    #####################################
    #
    # Rename flow run
    #
    #####################################
    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    (
        access_secret,
        access_token,
        consumer_key,
        consumer_secret,
        bearer_token,
    ) = get_credentials(secret_path="twitter_credentials", wait=None)

    cond = has_new_tweets(bearer_token, table_id=table_id)

    with case(cond, False):
        echo("No tweets to update")

    with case(cond, True):
        # pylint: disable=C0103
        filepath = crawler_metricas(
            access_secret,
            access_token,
            consumer_key,
            consumer_secret,
            upstream_tasks=[cond],
            table_id=table_id,
        )  # pylint: disable=C0103

        # pylint: disable=C0103
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_type="append",
            wait=filepath,
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

bd_twt_metricas.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
bd_twt_metricas.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
bd_twt_metricas.schedule = every_day


with Flow("br_bd_indicadores.twitter_metrics_agg") as bd_twt_metricas_agg:
    dataset_id = Parameter("dataset_id", default="br_bd_indicadores", required=True)
    table_id = Parameter("table_id", default="twitter_metrics_agg", required=True)

    current_flow_labels = get_current_flow_labels()
    materialization_flow = create_flow_run(
        flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
        project_name=constants.PREFECT_DEFAULT_PROJECT.value,
        parameters={
            "dataset_id": dataset_id,
            "table_id": table_id,
            "mode": materialization_mode,
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

bd_twt_metricas_agg.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
bd_twt_metricas_agg.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
bd_twt_metricas_agg.schedule = every_week
