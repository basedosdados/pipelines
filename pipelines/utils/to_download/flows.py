# -*- coding: utf-8 -*-
"""
Flows for to_download
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
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
            "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/bolsa-familia-pagamentos/202101_BolsaFamilia_Pagamentos.zip",
            "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/bolsa-familia-pagamentos/202102_BolsaFamilia_Pagamentos.zip",
            "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/bolsa-familia-pagamentos/202103_BolsaFamilia_Pagamentos.zip",
        ],
        required=False,
    )
    save_path = Parameter(
        "save_path",
        default="/tmp/data/teste1/",
        required=False,
    )
    file_type = Parameter("file_type", default="zip", required=False)
    to_download(url, save_path, file_type)

utils_to_download_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
utils_to_download_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
