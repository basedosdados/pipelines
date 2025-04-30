# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects of the pipelines
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_anatel_telefonia_movel project
    """

    COOEKIES = {
        "_ga": "GA1.1.1373815678.1744670764",
        "SLG_G_WPT_TO": "pt",
        "SLG_GWPT_Show_Hide_tmp": "1",
        "SLG_wptGlobTipTmp": "1",
        "AWSALB": "uNE7TsXIJeZE89WxJSn4deQ0LzA78gEi6OQY5DsOky3dxNNqzXdeZvhxAyskH0YwYUYdc1oJIDJKMIIoFm7cv0Q4Ox8l/wwPrnLuh9aeNQG5DV2hgLSpqIikwsWf",
        "AWSALBCORS": "uNE7TsXIJeZE89WxJSn4deQ0LzA78gEi6OQY5DsOky3dxNNqzXdeZvhxAyskH0YwYUYdc1oJIDJKMIIoFm7cv0Q4Ox8l/wwPrnLuh9aeNQG5DV2hgLSpqIikwsWf",
        "_ga_HVQVE1EE4Y": "GS1.1.1744841164.6.1.1744841288.59.0.0",
        "_ga_YEVH28106Q": "GS1.1.1744841164.6.1.1744841288.0.0.0",
        "_ga_Q5P3VN4T0E": "GS1.1.1744841164.6.1.1744841288.0.0.0",
        "_ga_PZGRWZP59S": "GS1.1.1744841164.6.1.1744841288.0.0.0",
        "_ga_3TJ75C1VW5": "GS1.1.1744841164.6.1.1744841288.0.0.0",
    }

    HEADERS = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        # 'cookie': '_ga=GA1.1.1373815678.1744670764; SLG_G_WPT_TO=pt; SLG_GWPT_Show_Hide_tmp=1; SLG_wptGlobTipTmp=1; AWSALB=uNE7TsXIJeZE89WxJSn4deQ0LzA78gEi6OQY5DsOky3dxNNqzXdeZvhxAyskH0YwYUYdc1oJIDJKMIIoFm7cv0Q4Ox8l/wwPrnLuh9aeNQG5DV2hgLSpqIikwsWf; AWSALBCORS=uNE7TsXIJeZE89WxJSn4deQ0LzA78gEi6OQY5DsOky3dxNNqzXdeZvhxAyskH0YwYUYdc1oJIDJKMIIoFm7cv0Q4Ox8l/wwPrnLuh9aeNQG5DV2hgLSpqIikwsWf; _ga_HVQVE1EE4Y=GS1.1.1744841164.6.1.1744841288.59.0.0; _ga_YEVH28106Q=GS1.1.1744841164.6.1.1744841288.0.0.0; _ga_Q5P3VN4T0E=GS1.1.1744841164.6.1.1744841288.0.0.0; _ga_PZGRWZP59S=GS1.1.1744841164.6.1.1744841288.0.0.0; _ga_3TJ75C1VW5=GS1.1.1744841164.6.1.1744841288.0.0.0',
        "priority": "u=1, i",
        "referer": "https://dados.gov.br/dados/conjuntos-dados/acessos-autorizadas-smp",
        "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "Opera GX";v="117"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 OPR/117.0.0.0",
    }

    URL = "https://dados.gov.br/dados/conjuntos-dados/acessos-autorizadas-smp"

    INPUT_PATH = "/tmp/data/input/"

    TABLES_OUTPUT_PATH = {
        "microdados": "/tmp/data/microdados/output/",
        "densidade_brasil": "/tmp/data/BRASIL/output/",
        "densidade_uf": "/tmp/data/UF/output/",
        "densidade_municipio": "/tmp/data/MUNICIPIO/output/",
    }

    # ? MICRODADOS

    ORDER_COLUMNS_MICRODADOS = [
        "ano",
        "mes",
        "sigla_uf",
        "ddd",
        "id_municipio",
        "cnpj",
        "empresa",
        "porte_empresa",
        "tecnologia",
        "sinal",
        "modalidade",
        "pessoa",
        "produto",
        "acessos",
    ]

    RENAME_COLUMNS_MICRODADOS = {
        "Ano": "ano",
        "Mês": "mes",
        "Grupo Econômico": "grupo_economico",
        "Empresa": "empresa",
        "CNPJ": "cnpj",
        "Porte da Prestadora": "porte_empresa",
        "UF": "sigla_uf",
        "Município": "municipio",
        "Código IBGE Município": "id_municipio",
        "Código Nacional": "ddd",
        "Código Nacional (Chip)": "ddd_chip",
        "Modalidade de Cobrança": "modalidade",
        "Tecnologia": "tecnologia",
        "Tecnologia Geração": "sinal",
        "Tipo de Pessoa": "pessoa",
        "Tipo de Produto": "produto",
        "Acessos": "acessos",
    }

    DROP_COLUMNS_MICRODADOS = ["grupo_economico", "municipio", "ddd_chip"]

    # ? --------------------------------------------- > Densidade Municipio

    ORDER_COLUMNS_MUNICIPIO = ["Ano", "Mês", "UF", "Código IBGE", "Densidade"]

    RENAME_COLUMNS_MUNICIPIO = {
        "Ano": "ano",
        "Mês": "mes",
        "UF": "sigla_uf",
        "Código IBGE Município": "id_municipio",
        "Densidade": "densidade",
    }

    # ? --------------------------------------------- > Densidade UF

    RENAME_COLUMNS_UF = {
        "Ano": "ano",
        "Mês": "mes",
        "UF": "sigla_uf",
        "Densidade": "densidade",
    }

    ORDER_COLUMNS_UF = ["Ano", "Mês", "UF", "Densidade"]

    # ? --------------------------------------------- > Densidade Brasil

    RENAME_COLUMNS_BRASIL = {
        "Ano": "ano",
        "Mês": "mes",
        "Densidade": "densidade",
    }

    ORDER_COLUMNS_BRASIL = ["Ano", "Mês", "Densidade"]
