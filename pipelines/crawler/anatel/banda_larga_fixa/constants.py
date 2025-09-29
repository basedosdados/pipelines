"""
Constant values for the datasets projects
"""

###############################################################################

from enum import Enum


class constants(Enum):
    URL = (
        "https://dados.gov.br/dados/conjuntos-dados/acessos---banda-larga-fixa"
    )

    COOKIES = {
        "_ga": "GA1.1.1373815678.1744670764",
        "SLG_G_WPT_TO": "pt",
        "SLG_GWPT_Show_Hide_tmp": "1",
        "SLG_wptGlobTipTmp": "1",
        "AWSALB": "VwPXBjkh3JMyPZnflBxvTLuhZrtgjJkUOAF2o3DLzPw91FLKfa46btmQBRooIJLmWoHj9ZgmprqHSkzmemH3wx1m9IXbzyemVzcLKYb9AioQ8F7vXVf/VtIf8Chu",
        "AWSALBCORS": "VwPXBjkh3JMyPZnflBxvTLuhZrtgjJkUOAF2o3DLzPw91FLKfa46btmQBRooIJLmWoHj9ZgmprqHSkzmemH3wx1m9IXbzyemVzcLKYb9AioQ8F7vXVf/VtIf8Chu",
        "_ga_HVQVE1EE4Y": "GS1.1.1744751022.3.1.1744751036.46.0.0",
        "_ga_YEVH28106Q": "GS1.1.1744751022.3.1.1744751036.0.0.0",
        "_ga_Q5P3VN4T0E": "GS1.1.1744751021.3.1.1744751036.0.0.0",
        "_ga_PZGRWZP59S": "GS1.1.1744751021.3.1.1744751037.0.0.0",
        "_ga_3TJ75C1VW5": "GS1.1.1744751022.3.1.1744751037.0.0.0",
    }

    HEADERS = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "cookie": "_ga=GA1.1.1373815678.1744670764; SLG_G_WPT_TO=pt; SLG_GWPT_Show_Hide_tmp=1; SLG_wptGlobTipTmp=1; AWSALB=VwPXBjkh3JMyPZnflBxvTLuhZrtgjJkUOAF2o3DLzPw91FLKfa46btmQBRooIJLmWoHj9ZgmprqHSkzmemH3wx1m9IXbzyemVzcLKYb9AioQ8F7vXVf/VtIf8Chu; AWSALBCORS=VwPXBjkh3JMyPZnflBxvTLuhZrtgjJkUOAF2o3DLzPw91FLKfa46btmQBRooIJLmWoHj9ZgmprqHSkzmemH3wx1m9IXbzyemVzcLKYb9AioQ8F7vXVf/VtIf8Chu; _ga_HVQVE1EE4Y=GS1.1.1744751022.3.1.1744751036.46.0.0; _ga_YEVH28106Q=GS1.1.1744751022.3.1.1744751036.0.0.0; _ga_Q5P3VN4T0E=GS1.1.1744751021.3.1.1744751036.0.0.0; _ga_PZGRWZP59S=GS1.1.1744751021.3.1.1744751037.0.0.0; _ga_3TJ75C1VW5=GS1.1.1744751022.3.1.1744751037.0.0.0",
        "priority": "u=1, i",
        "referer": "https://dados.gov.br/dados/conjuntos-dados/acessos---banda-larga-fixa",
        "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "Opera GX";v="117"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 OPR/117.0.0.0",
    }

    INPUT_PATH = "/tmp/data/input/"

    TABLES_OUTPUT_PATH = {
        "microdados": "/tmp/data/microdados/output/",
        "densidade_brasil": "/tmp/data/BRASIL/output/",
        "densidade_uf": "/tmp/data/UF/output/",
        "densidade_municipio": "/tmp/data/MUNICIPIO/output/",
    }

    # ! ------------> Microdados
    RENAME_MICRODADOS = {
        "Ano": "ano",
        "Mês": "mes",
        "Grupo Econômico": "grupo_economico",
        "Empresa": "empresa",
        "CNPJ": "cnpj",
        "Porte da Prestadora": "porte_empresa",
        "UF": "sigla_uf",
        "Município": "municipio",
        "Código IBGE Município": "id_municipio",
        "Faixa de Velocidade": "velocidade",
        "Tecnologia": "tecnologia",
        "Meio de Acesso": "transmissao",
        "Acessos": "acessos",
        "Tipo de Pessoa": "pessoa",
        "Tipo de Produto": "produto",
    }

    ORDER_COLUMNS_MICRODADOS = [
        "ano",
        "mes",
        "sigla_uf",
        "id_municipio",
        "cnpj",
        "empresa",
        "porte_empresa",
        "tecnologia",
        "transmissao",
        "velocidade",
        "produto",
        "acessos",
    ]

    SORT_VALUES_MICRODADOS = [
        "ano",
        "mes",
        "sigla_uf",
        "id_municipio",
        "cnpj",
        "empresa",
        "porte_empresa",
        "tecnologia",
        "transmissao",
        "velocidade",
    ]

    DROP_COLUMNS_MICRODADOS = ["grupo_economico", "municipio"]

    # ! ------------ > Densidade Brasil

    RENAME_COLUMNS_BRASIL = {
        "Ano": "ano",
        "Mês": "mes",
        "Densidade": "densidade",
    }

    DROP_COLUMNS_BRASIL = ["UF", "Município", "Geografia", "Código IBGE"]

    # ! --------------> Densidade UF

    RENAME_COLUMNS_UF = {
        "Ano": "ano",
        "Mês": "mes",
        "UF": "sigla_uf",
        "Densidade": "densidade",
    }

    DROP_COLUMNS_UF = ["Município", "Código IBGE", "Geografia"]

    # ! -----------------> Densidade Municipio

    RENAME_COLUMNS_MUNICIPIO = {
        "Ano": "ano",
        "Mês": "mes",
        "UF": "sigla_uf",
        "Código IBGE": "id_municipio",
        "Densidade": "densidade",
    }

    DROP_COLUMNS_MUNICIPIO = ["Município", "Geografia"]
