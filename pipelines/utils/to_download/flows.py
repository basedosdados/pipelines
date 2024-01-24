# -*- coding: utf-8 -*-
"""
Flows for to_download
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

# from pipelines.utils.to_download.schedules import every_two_weeks
from pipelines.utils.decorators import Flow
from pipelines.utils.to_download.tasks import to_download

with Flow(
    name="test_to_download_task",
    code_owners=[
        "arthurfg",
    ],
) as utils_to_download_flow:
    url = Parameter(
        "url",
        default=[
            "https://www.learningcontainer.com/download/sample-large-zip-file/?wpdmdl=1639&refresh=659ffb53e23891704983379",
            "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/cpgf/202302_CPGF.zip",
        ],
        required=False,
    )
    save_path = Parameter(
        "save_path",
        default=[
            "/tmp/data/teste1/",
            "/tmp/data/teste2/",
        ],
        required=False,
    )
    file_type = Parameter("file_type", default="zip", required=False)
    to_download(url, save_path, file_type)

utils_to_download_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
utils_to_download_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow.schedule = every_two_weeks
