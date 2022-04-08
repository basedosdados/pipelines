"""
Flows for br_twitter
"""
from prefect import Flow, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.datasets.bd_twitter.tasks import (
    crawler_metricas,
    crawler_metricas_agg,
    has_new_tweets,
    echo,
    get_credentials,
)
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    update_publish_sql,
    publish_table,
)
from pipelines.datasets.bd_twitter.schedules import every_day, every_week

with Flow("bd_twitter_data.metricas_tweets") as bd_twt_metricas:
    dataset_id = "bd_twitter"  # pylint: disable=C0103
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
            dump_type="overwrite",
            partitions=["upload_day"],
            wait=filepath,
        )

        # pylint: disable=C0103
        wait_update_publish_sql = update_publish_sql(
            dataset_id,
            table_id,
            dtype={
                "retweet_count": "INT64",
                "reply_count": "INT64",
                "like_count": "INT64",
                "quote_count": "INT64",
                "created_at": "STRING",
                "url_link_clicks": "FLOAT64",
                "user_profile_clicks": "FLOAT64",
                "impression_count": "FLOAT64",
            },
            upstream_tasks=[wait_upload_table],
        )

        publish_table(
            path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            if_exists="replace",
            wait=wait_update_publish_sql,
        )

bd_twt_metricas.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
bd_twt_metricas.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
bd_twt_metricas.schedule = every_day


with Flow("bd_twitter_data.metricas_tweets_agg") as bd_twt_metricas_agg:
    dataset_id = "bd_twitter"  # pylint: disable=C0103
    table_id = "metricas_tweets_agg"  # pylint: disable=C0103

    (
        access_secret,
        access_token,
        consumer_key,
        consumer_secret,
        bearer_token,
    ) = get_credentials(secret_path="twitter_credentials")

    # pylint: disable=C0103
    filepath = crawler_metricas_agg(
        access_secret, access_token, consumer_key, consumer_secret
    )

    # pylint: disable=C0103
    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type="overwrite",
        wait=filepath,
    )

    # pylint: disable=C0103
    wait_update_publish_sql = update_publish_sql(
        dataset_id,
        table_id,
        dtype={
            "retweet_count": "INT64",
            "reply_count": "INT64",
            "like_count": "INT64",
            "quote_count": "INT64",
            "date": "DATE",
            "url_link_clicks": "FLOAT64",
            "user_profile_clicks": "FLOAT64",
            "impression_count": "FLOAT64",
        },
        upstream_tasks=[wait_upload_table],
    )

    # pylint: disable=C0103
    publish_table(
        path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        if_exists="replace",
        wait=wait_update_publish_sql,
    )

bd_twt_metricas_agg.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
bd_twt_metricas_agg.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
bd_twt_metricas_agg.schedule = every_week
