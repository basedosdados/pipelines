# -*- coding: utf-8 -*-
"""
Constants for br_bcb_estban pipeline.
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    ESTBAN_URL = "https://www4.bcb.gov.br/fis/cosif/estban.asp?frame=1"
    ESTBAN_NEW_URL = "https://www.bcb.gov.br/estatisticas/estatisticabancariamunicipios"

    # TODO: rename xpath to css selector
    XPATH_INPUT_FIELD_DICT = {
        "municipio": "body > app-root > app-root > div > div > main > dynamic-comp > div > div:nth-child(5) > div:nth-child(1) > div > bcb-download-filter > div > ng-select > div > div > div.ng-input > input[type=text]",
        "agencia": "body > app-root > app-root > div > div > main > dynamic-comp > div > div:nth-child(5) > div:nth-child(2) > div > bcb-download-filter > div > ng-select > div > div > div.ng-input > input[type=text]",
    }

    XPATH_DOWNLOAD_BUTTON = {
        "municipio": "body > app-root > app-root > div > div > main > dynamic-comp > div > div:nth-child(5) > div:nth-child(1) > div > bcb-download-filter > div > button",
        "agencia": "body > app-root > app-root > div > div > main > dynamic-comp > div > div:nth-child(5) > div:nth-child(2) > div > bcb-download-filter > div > button",
    }

    ZIPFILE_PATH_MUNICIPIO = "/tmp/br_bcb_estban/municipio/zipfile"
    ZIPFILE_PATH_AGENCIA = "/tmp/br_bcb_estban/agencia/zipfile"
    INPUT_PATH_MUNICIPIO = "/tmp/br_bcb_estban/municipio/input"
    INPUT_PATH_AGENCIA = "/tmp/br_bcb_estban/agencia/input"
    OUTPUT_PATH_MUNICIPIO = "/tmp/br_bcb_estban/municipio/output/"
    OUTPUT_PATH_AGENCIA = "/tmp/br_bcb_estban/agencia/output/"
