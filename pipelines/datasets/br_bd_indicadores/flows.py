"""
Flows for br_bd_indicadores
"""
from copy import deepcopy

from prefect import Flow, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.datasets.br_bd_indicadores.tasks import (
    crawler_metricas,
    crawler_metricas_agg,
    has_new_tweets,
    echo,
    get_credentials,
)

from pipelines.utils.execute_dbt_model.flows import run_dbt_model_flow
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

run_dbt_br_bd_indicadores_flow = deepcopy(run_dbt_model_flow)
run_dbt_br_bd_indicadores_flow.name = "br_bd_indicadores - Materializar tabelas"
run_dbt_br_bd_indicadores_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
run_dbt_br_bd_indicadores_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
run_dbt_br_bd_indicadores_flow.schedule = every_day
