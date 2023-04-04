# -*- coding: utf-8 -*-
"""
Constants for br_tse_eleicoes pipeline.
"""

from enum import Enum

# todo: setar links de download da base em constants


class constants(Enum):  # pylint: disable=c0103

    """
    Constants for utils.
    """

    ESTBAN_URL = "https://www4.bcb.gov.br/fis/cosif/estban.asp?frame=1"
    AGENCIA_XPATH = '//*[@id="ESTBAN_MUNICIPIO"]'
    MUNICIPIO_XPATH = '//*[@id="ESTBAN_AGENCIA"]'
    DOWNLOAD_PATH_MUNICIPIO = "/tmp/input/municipio/"
    DOWNLOAD_PATH_AGENCIA = "/tmp/input/agencia/"
    CLEANED_FILES_PATH_MUNICIPIO = "/tmp/data/output/municipio/"
    CLEANED_FILES_PATH_AGENCIA = "/tmp/data/output/agencia/"
