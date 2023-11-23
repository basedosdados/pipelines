# -*- coding: utf-8 -*-
"""
Constants for br_bcb_agencia pipeline.
"""


from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    AGENCIA_URL = "https://www.bcb.gov.br/fis/info/agencias.asp?frame=1"
    AGENCIA_XPATH = "/html/body/div/table/tbody/tr[2]/td[2]/form/select"
    DOWNLOAD_PATH_AGENCIA = "/tmp/input/"
    CLEANED_FILES_PATH_AGENCIA = "/tmp/output/"
