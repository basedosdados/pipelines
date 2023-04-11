# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""
from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_inmet_bdmep project
    """

    COLUMNS_ORDER = [
        "data",
        "hora",
        "id_estacao",
        "precipitacao_total",
        "pressao_atm_hora",
        "pressao_atm_max",
        "pressao_atm_min",
        "radiacao_global",
        "temperatura_bulbo_hora",
        "temperatura_orvalho_hora",
        "temperatura_max",
        "temperatura_min",
        "temperatura_orvalho_max",
        "temperatura_orvalho_min",
        "umidade_rel_max",
        "umidade_rel_min",
        "umidade_rel_hora",
        "vento_direcao",
        "vento_rajada_max",
        "vento_velocidade",
    ]
