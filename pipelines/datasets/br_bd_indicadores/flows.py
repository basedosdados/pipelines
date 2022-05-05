"""
Flows for br_bd_indicadores
"""

from prefect import Flow, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.datasets.br_bd_indicadores.tasks import (
    crawler_metricas,
    crawler_metricas_agg,
    has_new_tweets,
    echo,
    get_credentials,
)

from pipelines.utils.tasks import create_table_and_upload_to_gcs
from pipelines.datasets.br_bd_indicadores.schedules import every_day, every_week

with Flow("br_bd_indicadores.metricas_tweets") as bd_twt_metricas:
    dataset_id = "br_bd_indicadores"  # pylint: disable=C0103
    table_id = "metricas_tweets"  # pylint: disable=C0103

    (
        access_secret,
        access_token,
        consumer_key,
        consumer_secret,
        bearer_token,
    ) = get_credentials(secret_path="twitter_credentials")

    cond = has_new_tweets(bearer_token)

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
        )  # pylint: disable=C0103

        # pylint: disable=C0103
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_type="append",
            wait=filepath,
        )

        bd_materialization_flow = create_flow_run(
        flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
        project_name=constants.PREFECT_DEFAULT_PROJECT.value,
        parameters={
            "dataset_id": dataset_id,
            "table_id": table_id,
            "mode": "dev",
        },
        labels=[
            constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
        ],
        run_name=f"Materialize: {dataset_id}.{table_id}",
        )

        wait_for_materialization = wait_for_flow_run(
        bd_materialization_flow,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
        )

bd_twt_metricas.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
bd_twt_metricas.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
bd_twt_metricas.schedule = every_day


with Flow("br_bd_indicadores.metricas_tweets_agg") as bd_twt_metricas_agg:
    dataset_id = "br_bd_indicadores"  # pylint: disable=C0103
    table_id = "metricas_tweets_agg"  # pylint: disable=C0103

    # pylint: disable=C0103
    filepath = crawler_metricas_agg()

    # pylint: disable=C0103
    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type="overwrite",
        wait=filepath,
    )

bd_twt_metricas_agg.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
bd_twt_metricas_agg.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
bd_twt_metricas_agg.schedule = every_week
