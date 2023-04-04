# -*- coding: utf-8 -*-
"""
Constants for br_tse_eleicoes pipeline.
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103

    """
    Constants for utils.
    """

    ESTBAN_URL = "https://www4.bcb.gov.br/fis/cosif/estban.asp?frame=1"
    AGENCIA_XPATH = '//*[@id="ESTBAN_AGENCIA"]'
    MUNICIPIO_XPATH = '//*[@id="ESTBAN_MUNICIPIO"]'
    DOWNLOAD_PATH_MUNICIPIO = "/tmp/input/municipio/"
    DOWNLOAD_PATH_AGENCIA = "/tmp/input/agencia/"
    CLEANED_FILES_PATH_MUNICIPIO = "/tmp/output/municipio/"
    CLEANED_FILES_PATH_AGENCIA = "/tmp/output/agencia/"
