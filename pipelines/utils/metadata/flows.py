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
    dataset_id = Parameter("dataset_id", default="br_inpe_prodes", required=True)
    table_id = Parameter("table_id", default="municipio_bioma", required=True)
    coverage_type = Parameter("coverage_type", required=False)
    date_format = Parameter("date_format", required=False)
    time_unit = Parameter("time_unit", required=False)
    time_delta = Parameter("time_delta", required=False)
    date_column_name = Parameter("date_column_name", required=False)
    materialization_mode = Parameter("materialization_mode", default="dev", required=False)
    api_mode = Parameter("api_mode", default="prod", required=False)
    bq_project = Parameter("bq_project", default="basedosdados", required=False)

    update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={
                        "year": "ano_referencia",
                        "month": "mes_referencia",
                    },
                    date_format="%Y-%m",
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
