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
from pipelines.utils.metadata.tasks import update_django_metadata

# from pipelines.utils.utils import log

with Flow(
    name="update_temporal_coverage_teste",
    code_owners=[
        "lauris",
    ],
) as temporal_coverage_updater_flow:
    dataset_id = Parameter("dataset_id", required=True)
    table_id = Parameter("table_id", required=True)
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )

    update_django_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        date_column_name={"date": "data_comunicado"},
        date_format="%Y-%m-%d",
        coverage_type="part_bdpro",
        time_delta={"months": 6},
        prefect_mode=materialization_mode,
        bq_project="basedosdados",
    )

temporal_coverage_updater_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
temporal_coverage_updater_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# flow.schedule = every_two_weeks
