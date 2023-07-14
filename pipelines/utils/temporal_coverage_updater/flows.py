# -*- coding: utf-8 -*-
"""
Flows for temporal_coverage_updater
"""

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.utils.tasks import (
    update_django_metadata,
)

# from pipelines.datasets.temporal_coverage_updater.schedules import every_two_weeks
from pipelines.utils.decorators import Flow
from prefect import Parameter

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
        dataset_id,
        table_id,
        metadata_type="DateTimeRange",
        bq_last_update=True,
    )
    # (email, password) = get_credentials(secret_path="api_user_prod")
    # ids = find_ids(
    #    dataset_id, table_id, email, password, upstream_tasks=[email, password]
    # )
    # last_date = extract_last_update(
    #    dataset_id, table_id, upstream_tasks=[ids, email, password]
    # )
    # first_date = get_first_date(
    #    ids, email, password, upstream_tasks=[ids, last_date, email, password]
    # )
    # update_temporal_coverage(
    #    ids,
    #    first_date,
    #    last_date,
    #    email,
    #    password,
    #    upstream_tasks=[ids, last_date, first_date, email, password],
    # )


temporal_coverage_updater_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
temporal_coverage_updater_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# flow.schedule = every_two_weeks
