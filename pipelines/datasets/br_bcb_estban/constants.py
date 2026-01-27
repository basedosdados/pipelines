"""
Constants for br_bcb_estban pipeline.
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
    GUID_LISTA = "f6391806-fd85-43af-acf1-c86d5b8dd6df"
    TRONCO = "estatisticas"
    PASTAS = {"agencia": "agencia", "municipio": "municipio"}
    BASE_DOWNLOAD_URL = "https://www.bcb.gov.br"

    # ==== Configs for municipio and agencia tables ==== #
    TABLES_CONFIGS = {
        "agencia": {
            "pasta": "agencia",
            "zipfile_path": "/tmp/br_bcb_estban/agencia/zipfile",
            "input_path": "/tmp/br_bcb_estban/agencia/input",
            "output_path": "/tmp/br_bcb_estban/agencia/output/",
            "rename_mapping": {
                "#DATA_BASE": "data_base",
                "UF": "sigla_uf",
                "CNPJ": "cnpj_basico",
                "MUNICIPIO": "municipio",
                "CODMUN": "id_municipio_bcb",
                "CODMUN_IBGE": "id_municipio_original",
                "NOME_INSTITUICAO": "instituicao",
                "AGENCIA": "cnpj_agencia",
            },
            "order": [
                "ano",
                "mes",
                "sigla_uf",
                "id_municipio",
                "cnpj_basico",
                "instituicao",
                "cnpj_agencia",
                "id_verbete",
                "valor",
            ],
        },
        "municipio": {
            "pasta": "municipio",
            "zipfile_path": "/tmp/br_bcb_estban/municipio/zipfile",
            "input_path": "/tmp/br_bcb_estban/municipio/input",
            "output_path": "/tmp/br_bcb_estban/municipio/output/",
            "rename_mapping": {
                "#DATA_BASE": "data_base",
                "UF": "sigla_uf",
                "CNPJ": "cnpj_basico",
                "MUNICIPIO": "municipio",
                "CODMUN": "id_municipio_bcb",
                "CODMUN_IBGE": "id_municipio_original",
                "NOME_INSTITUICAO": "instituicao",
                "AGEN_ESPERADAS": "agencias_esperadas",
                "AGEN_PROCESSADAS": "agencias_processadas",
            },
            "order": [
                "ano",
                "mes",
                "sigla_uf",
                "id_municipio",
                "cnpj_basico",
                "instituicao",
                "agencias_esperadas",
                "agencias_processadas",
                "id_verbete",
                "valor",
            ],
        },
    }
