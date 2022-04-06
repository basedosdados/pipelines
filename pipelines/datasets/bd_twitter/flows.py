"""
Flows for br_cvm_oferta_publica_distribuicao
"""
# pylint: disable=C0103, E1123, invalid-name
from datetime import datetime

from prefect import Flow, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.datasets.bd_twitter.tasks import crawler_metricas, crawler_metricas_agg, has_new_tweets, echo
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    update_publish_sql,
    publish_table,
)
from pipelines.utils.utils import log
from pipelines.datasets.bd_twitter.schedules import every_day, every_week

with Flow("bd_twitter_data.metricas_tweets") as bd_twt_metricas:
    dataset_id = "bd_twitter"
    table_id = "metricas_tweets"

    cond = has_new_tweets()

    with case(cond, False):
        echo("No tweets to update")

    with case(cond, True):
        filepath = crawler_metricas(upstream_tasks=[cond])

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_type="overwrite",
            wait=filepath,
        )

        wait_update_publish_sql = update_publish_sql(dataset_id, table_id, dtype= {
            'retweet_count': 'INT64', 
            'reply_count': 'INT64', 
            'like_count': 'INT64', 
            'quote_count': 'INT64',
        'created_at': 'STRING',
        'url_link_clicks': 'INT64', 
        'user_profile_clicks': 'INT64',
        'impression_count': 'INT64'
        }, upstream_tasks=[wait_upload_table])

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
    dataset_id = "bd_twitter"
    table_id = "metricas_tweets_agg"

    filepath = crawler_metricas_agg()

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type="overwrite",
        wait=filepath,
    )

    wait_update_publish_sql = update_publish_sql(dataset_id, table_id, dtype= {
        'retweet_count': 'INT64', 
        'reply_count': 'INT64', 
        'like_count': 'INT64', 
        'quote_count': 'INT64',
       'date': 'DATE',
       'url_link_clicks': 'INT64', 
       'user_profile_clicks': 'INT64',
       'impression_count': 'INT64'
    }, upstream_tasks=[wait_upload_table])

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
