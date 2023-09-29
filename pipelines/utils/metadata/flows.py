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
    _last_date = Parameter("_last_date", default="2023-01-01", required=True)
    bq_table_last_year_month = Parameter(
        "bq_table_last_year_month", default=False, required=True
    )
    bq_last_update = Parameter("bq_last_update", default=False, required=True)
    is_bd_pro = Parameter("is_bd_pro", default=False, required=True)
    is_free = Parameter("is_free", default=True, required=True)
    date_format = Parameter("date_format", default="yy-mm-dd", required=True)
    api_mode = Parameter("api_mode", default="prod", required=True)
    time_delta = Parameter("time_delta", default=None, required=True)
    time_unit = Parameter("time_unit", default=None, required=True)
    billing_project_id = Parameter(
        "billing_project_id", default="basedosdados-dev", required=True
    )

    date = get_today_date()
    update_django_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        metadata_type="DateTimeRange",
        _last_date=_last_date,
        bq_table_last_year_month=bq_table_last_year_month,
        bq_last_update=bq_last_update,
        is_bd_pro=is_bd_pro,
        is_free=is_free,
        date_format=date_format,
        api_mode=api_mode,
        billing_project_id=billing_project_id,
    )


temporal_coverage_updater_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
temporal_coverage_updater_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# flow.schedule = every_two_weeks
