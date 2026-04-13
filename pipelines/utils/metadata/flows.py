"""
Flows for temporal_coverage_updater
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import (
    update_django_metadata,
)

with Flow(
    name="update_temporal_coverage",
    code_owners=[
        "lauris",
    ],
) as temporal_coverage_updater_flow:
    dataset_id = Parameter("dataset_id", required=True)
    table_id = Parameter("table_id", required=True)

    coverage_type = Parameter(
        "coverage_type", default="part_bdpro", required=False
    )
    date_column_name = Parameter(
        "date_column_name",
        default={"month": "mes", "year": "ano"},
        required=False,
    )
    date_format = Parameter("date_format", default="%Y-%m", required=False)
    time_delta = Parameter("time_delta", default={"months": 6}, required=False)

    update_django_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        date_column_name=date_column_name,
        date_format=date_format,
        coverage_type=coverage_type,
        time_delta=time_delta,
        bq_project="basedosdados",
    )

temporal_coverage_updater_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
temporal_coverage_updater_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# flow.schedule = every_two_weeks
