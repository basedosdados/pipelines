"""
Constantes de br_ibge_ppm.
"""

from enum import Enum


class constants(Enum):
    """Constantes de br_ibge_ppm."""

    SOURCE_LINK = "https://sidra.ibge.gov.br/pesquisa/ppm/tabelas"
    METADATA_LINK = "https://servicodados.ibge.gov.br/api/v3/agregados/{agregado}/metadados"
    SERIES_LINK = (
        "https://servicodados.ibge.gov.br/api/v3/agregados/{agregado}"
        "/periodos/{ano}/variaveis/{variavel}?localidades=N6[all]"
    )
    CLASSIFICATION_PARAM = "&classificacao={classificacao}[{categoria}]"

    PATH = "/tmp/br_ibge_ppm/"

    # O que a API devolve no lugar de um número: sem informação, valor
    # arredondado a zero, dado omitido por sigilo.
    NULL_VALUES = ("-", "..", "...", "X", "")

    # Unidades da variável 215 (valor da produção). A coluna `unidade` descreve a
    # quantidade produzida, então a moeda do ano não entra nela.
    MONETARY_UNITS = (
        "Mil Cruzeiros",
        "Mil Cruzados",
        "Mil Cruzados Novos",
        "Mil Cruzeiros Reais",
        "Mil Reais",
    )

    # Uma entrada por tabela. `series` lista as requisições que compõem a tabela:
    # cada uma é um par agregado/variável do SIDRA e a coluna que ela alimenta.
    # `unit_column` marca a variável de onde sai a coluna `unidade`.
    #
    # As categorias são as da classificação, menos os subtotais que a API
    # publica junto: `0` (Total) nas três classificações e `79366` (Peixes) na
    # aquicultura, que somam as categorias seguintes e dobrariam a produção.
    TABLES = {
        "efetivo_rebanhos": {
            "first_year": 1974,
            "label_column": "tipo_rebanho",
            "partition_columns": ["ano"],
            "columns": [
                "ano",
                "sigla_uf",
                "id_municipio",
                "tipo_rebanho",
                "quantidade",
            ],
            "series": [
                {
                    "agregado": "3939",
                    "variavel": "105",
                    "column": "quantidade",
                    "classificacao": "79",
                    "categorias": [
                        "2670",
                        "2675",
                        "2672",
                        "32794",
                        "32795",
                        "2681",
                        "2677",
                        "32796",
                        "32793",
                        "2680",
                    ],
                },
            ],
        },
        "producao_origem_animal": {
            "first_year": 1974,
            "label_column": "produto",
            "partition_columns": ["ano"],
            "columns": [
                "ano",
                "sigla_uf",
                "id_municipio",
                "produto",
                "unidade",
                "quantidade",
                "valor",
            ],
            "series": [
                {
                    "agregado": "74",
                    "variavel": "106",
                    "column": "quantidade",
                    "classificacao": "80",
                    "categorias": [
                        "2682",
                        "2685",
                        "2686",
                        "2687",
                        "2683",
                        "2684",
                    ],
                    "unit_column": "unidade",
                },
                {
                    "agregado": "74",
                    "variavel": "215",
                    "column": "valor",
                    "classificacao": "80",
                    "categorias": [
                        "2682",
                        "2685",
                        "2686",
                        "2687",
                        "2683",
                        "2684",
                    ],
                },
            ],
        },
        "producao_aquicultura": {
            "first_year": 2013,
            "label_column": "produto",
            "partition_columns": ["ano"],
            "columns": [
                "ano",
                "sigla_uf",
                "id_municipio",
                "produto",
                "quantidade",
                "valor",
            ],
            "series": [
                {
                    "agregado": "3940",
                    "variavel": "4146",
                    "column": "quantidade",
                    "classificacao": "654",
                    "categorias": [
                        "32861",
                        "32865",
                        "32866",
                        "32867",
                        "32868",
                        "32869",
                        "32870",
                        "32871",
                        "32872",
                        "32873",
                        "32874",
                        "32875",
                        "32876",
                        "32877",
                        "32878",
                        "32879",
                        "32880",
                        "32881",
                        "32886",
                        "32887",
                        "32888",
                        "32889",
                        "32890",
                        "32891",
                    ],
                },
                {
                    "agregado": "3940",
                    "variavel": "215",
                    "column": "valor",
                    "classificacao": "654",
                    "categorias": [
                        "32861",
                        "32865",
                        "32866",
                        "32867",
                        "32868",
                        "32869",
                        "32870",
                        "32871",
                        "32872",
                        "32873",
                        "32874",
                        "32875",
                        "32876",
                        "32877",
                        "32878",
                        "32879",
                        "32880",
                        "32881",
                        "32886",
                        "32887",
                        "32888",
                        "32889",
                        "32890",
                        "32891",
                    ],
                },
            ],
        },
        "producao_pecuaria": {
            "first_year": 1974,
            "label_column": None,
            "partition_columns": ["ano"],
            "columns": [
                "ano",
                "sigla_uf",
                "id_municipio",
                "ovinos_tosquiados",
                "vacas_ordenhadas",
            ],
            "series": [
                {
                    "agregado": "95",
                    "variavel": "108",
                    "column": "ovinos_tosquiados",
                },
                {
                    "agregado": "94",
                    "variavel": "107",
                    "column": "vacas_ordenhadas",
                },
            ],
        },
    }
