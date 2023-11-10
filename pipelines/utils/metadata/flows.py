# -*- coding: utf-8 -*-
"""
Flows for temporal_coverage_updater
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

# from pipelines.datasets.temporal_coverage_updater.schedules import every_two_weeks
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import (
    get_today_date,
    test_ids,
    update_django_metadata,
)

# from pipelines.utils.utils import log

with Flow(
    name="update_temporal_coverage_teste",
    code_owners=[
        "arthurfg",
    ],
) as temporal_coverage_updater_flow:
    dataset_id = Parameter("dataset_id", default="test_dataset", required=True)
    table_id = Parameter("table_id", default="test_laura_student", required=True)


    update_django_metadata(
        dataset_id = dataset_id,
        table_id = table_id
    )


temporal_coverage_updater_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
temporal_coverage_updater_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# flow.schedule = every_two_weeks
