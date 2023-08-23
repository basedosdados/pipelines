# -*- coding: utf-8 -*-
"""
Flows for temporal_coverage_updater
"""

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.utils.metadata.tasks import (
    update_django_metadata,
    get_today_date,
    test_ids,
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
    date = get_today_date()
    # test_ids(dataset_id, table_id, api_mode="prod")
    # date = get_today_date
    update_django_metadata(
        dataset_id,
        table_id,
        metadata_type="DateTimeRange",
        _last_date=date,
        bq_table_last_year_month=False,
        bq_last_update=False,
        is_bd_pro=True,
        is_free=True,
        date_format="yy-mm-dd",
        api_mode="prod",
        time_delta=25,
        time_unit="days",
    )


temporal_coverage_updater_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
temporal_coverage_updater_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# flow.schedule = every_two_weeks
