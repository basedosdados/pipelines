# -*- coding: utf-8 -*-
"""
Flows for temporal_coverage_updater
"""

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.utils.temporal_coverage_updater.tasks import (
    find_ids,
    extract_last_update,
    get_first_date,
    update_temporal_coverage,
)

# from pipelines.datasets.temporal_coverage_updater.schedules import every_two_weeks
from pipelines.utils.decorators import Flow
from prefect import Parameter
from pipelines.utils.utils import log

with Flow(
    name="update_temporal_coverage_teste",
    code_owners=[
        "arthurfg",
    ],
) as temporal_coverage_updater_flow:
    dataset_id = Parameter("dataset_id", default="test_dataset", required=True)
    table_id = Parameter("table_id", default="test_laura_student", required=True)

    ids = find_ids(dataset_id, table_id)
    last_date = extract_last_update(dataset_id, table_id, upstream_tasks=[ids])
    first_date = get_first_date(ids, upstream_tasks=[ids, last_date])
    update_temporal_coverage(
        ids, first_date, last_date, upstream_tasks=[ids, last_date, first_date]
    )


temporal_coverage_updater_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
temporal_coverage_updater_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# flow.schedule = every_two_weeks
