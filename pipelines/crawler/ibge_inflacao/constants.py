# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""

from enum import Enum


class constants(Enum):
    DATASETS = {
        "br_ibge_ipca15": {
            "aggregates": [7062],
            "variables": [355, 356, 1120, 357],
        },
        "br_ibge_ipca": {
            "aggregates": [7060],
            "variables": [63, 69, 2265, 66],
        },
        "br_ibge_inpc": {
            "aggregates": [7063],
            "variables": [44, 68, 2292, 45],
        },
    }

    DATASETS_MES_BRASIL = {
        "br_ibge_ipca15": {
            "aggregates": [
                3065
            ],  # IPCA15 - Série histórica com número-índice, variação mensal e variações acumuladas em 3 meses, em 6 meses, no ano e em 12 meses (a partir de maio/2000)
            "variables": [
                1117,  # - IPCA15 - Número-índice (base: dezembro de 1993 = 100)
                355,  # - IPCA15 - Variação mensal
                1118,  # - IPCA15 - Variação acumulada em 3 meses
                1119,  # - IPCA15 - Variação acumulada em 6 meses
                356,  # - IPCA15 - Variação acumulada no ano
                1120,  # - IPCA15 - Variação acumulada em 12 meses
            ],
        },
        "br_ibge_ipca": {
            "aggregates": [
                1737
            ],  # IPCA - Série histórica com número-índice, variação mensal e variações acumuladas em 3 meses, em 6 meses, no ano e em 12 meses (a partir de dezembro/1979)
            "variables": [
                2266,  # - IPCA - Número-índice (base: dezembro de 1993 = 100)
                63,  # - IPCA - Variação mensal
                2263,  # - IPCA - Variação acumulada em 3 meses
                2264,  # - IPCA - Variação acumulada em 6 meses
                69,  # - IPCA - Variação acumulada no ano
                2265,  # - IPCA - Variação acumulada em 12 meses
            ],
        },
        "br_ibge_inpc": {
            "aggregates": [
                1736
            ],  # INPC - Série histórica com número-índice, variação mensal e variações acumuladas em 3 meses, em 6 meses, no ano e em 12 meses (a partir de abril/1979)
            "variables": [
                2289,  # - INPC - Número-índice (base: dezembro de 1993 = 100)
                44,  # - INPC - Variação mensal
                2290,  # - INPC - Variação acumulada em 3 meses
                2291,  # - INPC - Variação acumulada em 6 meses
                68,  # - INPC - Variação acumulada no ano
                2292,  # - INPC - Variação acumulada em 12 meses
            ],
        },
    }

    GEO_LEVELS_MES_BRASIL = "N1[all]"

    GEO_LEVELS = {
        "mes_categoria_brasil": "N1[all]",  # Brasil
        "mes_categoria_municipio": "N6[all]",  # Município
        "mes_categoria_rm": "N7[all]",  # Região Metropolitana
    }

    OUTPUT = "tmp/output"
    INPUT = "tmp/json"
