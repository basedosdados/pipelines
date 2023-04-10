# -*- coding: utf-8 -*-
"""
Flows for br_ms_cnes
"""


from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.datasets.br_ms_cnes.tasks import access_datasus_cnes_ftp
from pipelines.utils.decorators import Flow

with Flow(
    name="br_ms_cnes.estabelecimentos",
    code_owners=[
        "Gabriel Pisa",
    ],
) as datasets_br_ms_cnes_flow:

    access_datasus_cnes_ftp()

datasets_br_ms_cnes_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
datasets_br_ms_cnes_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow.schedule = every_two_weeks
