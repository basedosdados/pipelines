# -*- coding: utf-8 -*-
"""
Flows for botdosdados
"""
from prefect import Flow, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.datasets.botdosdados.tasks import (
    was_table_updated,
    get_credentials,
    send_tweet,
    echo,
)
from pipelines.datasets.botdosdados.schedules import every_hour

with Flow("botdosdados.send_tweets") as bot_dados_flow:

    # pylint: disable=C0103
    cond = was_table_updated(page_size=100, hours=1)

    with case(cond, False):
        echo("No table updated")

    with case(cond, True):
        (
            access_token_secret,
            access_token,
            consumer_key,
            consumer_secret,
            bearer_token,
        ) = get_credentials(
            secret_path="botdosdados_credentials", upstream_tasks=[cond]
        )

        send_tweet(
            access_token,
            access_token_secret,
            consumer_key,
            consumer_secret,
            bearer_token,
            upstream_tasks=[access_token],
        )  # pylint: disable=C0103

bot_dados_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
bot_dados_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
bot_dados_flow.schedule = every_hour
