# -*- coding: utf-8 -*-
"""
Constants for br_bcb_estban pipeline.
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    ESTBAN_URL = "https://www4.bcb.gov.br/fis/cosif/estban.asp?frame=1"
    ESTBAN_NEW_URL = (
        "https://www.bcb.gov.br/estatisticas/estatisticabancariamunicipios"
    )

    CSS_INPUT_FIELD_DICT = {
        "municipio": "/html/body/app-root/app-root/div/div/main/dynamic-comp/div/div[4]/div[1]/div/bcb-download-filter/div/ng-select/div/div/div[2]/input",
        "agencia": "/html/body/app-root/app-root/div/div/main/dynamic-comp/div/div[4]/div[2]/div/bcb-download-filter/div/ng-select/div/div/div[2]/input",
    }

    CSS_DOWNLOAD_BUTTON = {
        "municipio": "/html/body/app-root/app-root/div/div/main/dynamic-comp/div/div[4]/div[1]/div/bcb-download-filter/div/button",
        "agencia": "/html/body/app-root/app-root/div/div/main/dynamic-comp/div/div[4]/div[2]/div/bcb-download-filter/div/button",
    }

    ZIPFILE_PATH_MUNICIPIO = "/tmp/br_bcb_estban/municipio/zipfile"
    ZIPFILE_PATH_AGENCIA = "/tmp/br_bcb_estban/agencia/zipfile"
    INPUT_PATH_MUNICIPIO = "/tmp/br_bcb_estban/municipio/input"
    INPUT_PATH_AGENCIA = "/tmp/br_bcb_estban/agencia/input"
    OUTPUT_PATH_MUNICIPIO = "/tmp/br_bcb_estban/municipio/output/"
    OUTPUT_PATH_AGENCIA = "/tmp/br_bcb_estban/agencia/output/"
