"""
Constantes de br_bndes_desembolsos.

O contexto da fonte e as decisões de modelagem estão no README do conjunto.
"""

from enum import Enum


class constants(Enum):
    """Constantes de br_bndes_desembolsos."""

    RESOURCE_SHOW_URL = (
        "https://dadosabertos.bndes.gov.br/api/3/action/resource_show"
    )

    MENSAL_CKAN_RESOURCE_ID = "179950b8-b504-4cc7-b0db-9c9eed99e9ba"
    MENSAL_PATH = "/tmp/br_bndes_desembolsos/mensal/"
    MENSAL_CSV_FILENAME = "desembolsos-mensais.csv"
    CHUNKSIZE = 500_000

    SEPARATOR = ";"
    ENCODING = "cp1252"

    RENAME = {
        "forma_de_apoio": "forma_apoio",
        "inovacao": "indicador_inovacao",
        "porte_de_empresa": "porte_empresa",
        "uf": "sigla_uf",
        "municipio_codigo": "id_municipio",
        "desembolsos_reais": "valor_desembolsado",
    }

    # A fonte publica a UF por extenso, em caixa alta e sem acento.
    SIGLA_UF = {
        "ACRE": "AC",
        "ALAGOAS": "AL",
        "AMAPA": "AP",
        "AMAZONAS": "AM",
        "BAHIA": "BA",
        "CEARA": "CE",
        "DISTRITO FEDERAL": "DF",
        "ESPIRITO SANTO": "ES",
        "GOIAS": "GO",
        "MARANHAO": "MA",
        "MATO GROSSO": "MT",
        "MATO GROSSO DO SUL": "MS",
        "MINAS GERAIS": "MG",
        "PARA": "PA",
        "PARAIBA": "PB",
        "PARANA": "PR",
        "PERNAMBUCO": "PE",
        "PIAUI": "PI",
        "RIO DE JANEIRO": "RJ",
        "RIO GRANDE DO NORTE": "RN",
        "RIO GRANDE DO SUL": "RS",
        "RONDONIA": "RO",
        "RORAIMA": "RR",
        "SANTA CATARINA": "SC",
        "SAO PAULO": "SP",
        "SERGIPE": "SE",
        "TOCANTINS": "TO",
    }

    # Código que a fonte usa com o município "DIVERSOS".
    ID_MUNICIPIO_DIVERSOS = "9999998"

    PARTITION_COLUMNS = ["ano"]

    COLUMNS = [
        "mes",
        "sigla_uf",
        "id_municipio",
        "forma_apoio",
        "produto",
        "instrumento_financeiro",
        "indicador_inovacao",
        "porte_empresa",
        "setor_cnae",
        "subsetor_cnae_agrupado",
        "setor_bndes",
        "subsetor_bndes",
        "valor_desembolsado",
    ]

    MAPEAMENTO_BNDES_CNAE_CKAN_RESOURCE_ID = (
        "94ca01fe-7554-49a2-9200-e9625a304b88"
    )
    MAPEAMENTO_BNDES_CNAE_PATH = (
        "/tmp/br_bndes_desembolsos/mapeamento_bndes_cnae/"
    )
    MAPEAMENTO_BNDES_CNAE_CSV_FILENAME = "mapeamento-bndes-cnae.csv"

    MAPEAMENTO_BNDES_CNAE_RENAME = {
        "setor_cnae_classificacao_bndes": "setor_cnae",
        "subsetor_cnae_agrupado_classificacao_bndes": "subsetor_cnae_agrupado",
        "codigo_cnae_ibge": "codigo_cnae",
        "produto_bndes": "regra_produto",
    }

    MAPEAMENTO_BNDES_CNAE_REMAINDER_MARKER = "(restante)"

    MAPEAMENTO_BNDES_CNAE_CATEGORICAL_COLUMNS = [
        "regra_produto",
        "setor_cnae",
        "subsetor_cnae_agrupado",
        "setor_bndes",
        "subsetor_bndes",
    ]

    MAPEAMENTO_BNDES_CNAE_ACCENTS = {
        "eletrico": "elétrico",
        "eletronico": "eletrônico",
    }

    MAPEAMENTO_BNDES_CNAE_COLUMNS = [
        "cnae_2_secao",
        "cnae_2_divisao",
        "cnae_2_grupo",
        "cnae_2_classe",
        "cnae_2_subclasse",
        "regra_produto",
        "setor_cnae",
        "subsetor_cnae_agrupado",
        "setor_bndes",
        "subsetor_bndes",
    ]
