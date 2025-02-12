# -*- coding: utf-8 -*-
"""
Constants for br_bcb_agencia pipeline.
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    AGENCIA_URL = (
        "https://www.bcb.gov.br/estabilidadefinanceira/agenciasconsorcio"
    )

    CSS_INPUT_FIELD_DICT = {
        "agencia": "body > app-root > app-root > div > div > main > dynamic-comp > div > div:nth-child(4) > div.col-md-8 > div > bcb-download-filter:nth-child(8) > div > ng-select > div > div > div.ng-input > input[type=text]",
    }

    CSS_DOWNLOAD_BUTTON = {
        "agencia": "body > app-root > app-root > div > div > main > dynamic-comp > div > div:nth-child(4) > div.col-md-8 > div > bcb-download-filter:nth-child(8) > div > button",
    }

    ZIPFILE_PATH_AGENCIA = "/tmp/br_bcb_agencia/agencia/zipfile"
    INPUT_PATH_AGENCIA = "/tmp/br_bcb_agencia/agencia/input"
    OUTPUT_PATH_AGENCIA = "/tmp/br_bcb_agencia/agencia/output/"
