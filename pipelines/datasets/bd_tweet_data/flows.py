"""
Flows for bd_tweet_data
"""

from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.datasets.bd_tweet_data.tasks import say_hello
# from pipelines.datasets.bd_tweet_data.schedules import every_two_weeks

with Flow("my_flow") as flow:
    say_hello()

flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow.schedule = every_two_weeks
