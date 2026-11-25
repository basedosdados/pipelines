"""
Constantes de br_bndes_desembolsos.

O contexto da fonte e as decisões de modelagem estão no README do conjunto.
"""

from enum import Enum


class constants(Enum):
    """Constantes de br_bndes_desembolsos."""

    CKAN_RESOURCE_ID = "179950b8-b504-4cc7-b0db-9c9eed99e9ba"
    RESOURCE_SHOW_URL = (
        "https://dadosabertos.bndes.gov.br/api/3/action/resource_show"
    )

    PATH = "/tmp/br_bndes_desembolsos/mensal/"
    CSV_FILENAME = "desembolsos-mensais.csv"
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
