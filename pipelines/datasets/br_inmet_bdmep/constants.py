"""
Constant values for the datasets projects
"""

from enum import Enum
from pathlib import Path


class ConstantsMicrodados(Enum):
    """
    Constant values for the br_inmet_bdmep project
    """

    URL = "https://portal.inmet.gov.br/dadoshistoricos/"

    BASE_PATH = Path("tmp") / "data" / "microdados"

    PATH_INPUT = BASE_PATH / "input"

    PATH_OUTPUT = BASE_PATH / "output"

    PATH_REGEX = str(PATH_INPUT) + "/*.CSV"

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
