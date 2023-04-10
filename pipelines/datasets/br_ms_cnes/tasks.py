# -*- coding: utf-8 -*-
"""
Tasks for br_ms_cnes
"""


from prefect import task
from datetime import timedelta
from pipelines.utils.utils import log
from pipelines.constants import constants

import wget
import os


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def access_datasus_cnes_ftp() -> str:
    """
    Access data from datasus.gov.br
    """

    url = (
        "ftp://ftp.datasus.gov.br/dissemin/publicos/CNES/200508_/Dados/ST/STRJ1201.dbc"
    )
    filename = "STRJ1201.dbc"
    folder = "/tmp/data/"

    # build dir
    os.system(f"mkdir -p {folder}")

    # access ftp
    wget.download(url, filename, out="/tmp/data/")

    # list downloaded files
    files = os.listdir("/tmp/data/")

    log(f"The file {files} is in the {folder}")
