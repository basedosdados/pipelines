# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""

from enum import Enum


class constants(Enum):
    DATASETS = {
        "br_ibge_ipca15": {
            "aggregates": [7062],
            "variables": [
                355,
                356,
                1120,
                357,
            ],  # mensal, acumulado ano, acumulado 12m, peso
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

    GEO_LEVELS = {
        "mes_categoria_brasil": "N1[all]",  # Brasil
        "mes_categoria_municipio": "N6[all]",  # Município
        "mes_categoria_rm": "N7[all]",  # Região Metropolitana
    }
