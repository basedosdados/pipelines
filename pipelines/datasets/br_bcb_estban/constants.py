# -*- coding: utf-8 -*-
"""
Constants for br_bcb_estban pipeline.
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    ESTBAN_URL = "https://www4.bcb.gov.br/fis/cosif/estban.asp?frame=1"
    ESTBAN_NEW_URL = "https://www.bcb.gov.br/estatisticas/estatisticabancariamunicipios"
    AGENCIA_XPATH = '//*[@id="ESTBAN_AGENCIA"]'
    MUNICIPIO_XPATH = '//*[@id="ESTBAN_MUNICIPIO"]'

    XPATH_INPUT_FIELD_DICT = {
        "municipio": "body > app-root > app-root > div > div > main > dynamic-comp > div > div:nth-child(5) > div:nth-child(1) > div > bcb-download-filter > div > ng-select > div > div > div.ng-input > input[type=text]",
        "agencia": "/html/body/app-root/app-root/div/div/main/dynamic-comp/div/div[4]/div[2]/div/bcb-download-filter/div/ng-select/div/div/div[2]/input",
    }

    XPATH_DOWNLOAD_BUTTON = {
        "municipio": "body > app-root > app-root > div > div > main > dynamic-comp > div > div:nth-child(5) > div:nth-child(1) > div > bcb-download-filter > div > button",
        "agencia": "/html/body/app-root/app-root/div/div/main/dynamic-comp/div/div[4]/div[2]/div/bcb-download-filter/div/button",
    }

    DOWNLOAD_PATH_MUNICIPIO = "/tmp/input/municipio/"
    DOWNLOAD_PATH_AGENCIA = "/tmp/input/agencia/"
    CLEANED_FILES_PATH_MUNICIPIO = "/tmp/output/municipio/"
    CLEANED_FILES_PATH_AGENCIA = "/tmp/output/agencia/"
