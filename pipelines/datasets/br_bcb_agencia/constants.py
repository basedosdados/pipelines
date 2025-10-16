"""
Constants for br_bcb_agencia pipeline.
"""

from enum import Enum


class constants(Enum):
    # ==== General tasks constants ==== #
    HEADERS = {
        "sec-ch-ua-platform": '"Windows"',
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/139.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
        "sec-ch-ua-mobile": "?0",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "host": "www.bcb.gov.br",
    }

    # ==== BCB API constants ==== #
    BASE_URL = (
        "https://www.bcb.gov.br/api/servico/sitebcb/Documentos/byListGuid"
    )
    GUID_LISTA = "50f14992-f03c-4c47-8576-b96a65565485"
    TRONCO = "estabilidadefinanceira"
    PASTA = "/agencias"
    BASE_DOWNLOAD_URL = "https://www.bcb.gov.br"

    # ==== Paths constants ==== #
    ZIPFILE_PATH_AGENCIA = "/tmp/br_bcb_agencia/agencia/zipfile"
    INPUT_PATH_AGENCIA = "/tmp/br_bcb_agencia/agencia/input"
    OUTPUT_PATH_AGENCIA = "/tmp/br_bcb_agencia/agencia/output/"
