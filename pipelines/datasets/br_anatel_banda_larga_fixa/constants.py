# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_anatel_telefonia_movel project
    """

    URL = "https://www.anatel.gov.br/dadosabertos/paineis_de_dados/acessos/acessos_banda_larga_fixa.zip"

    INPUT_PATH = "/tmp/data/input/"

    OUTPUT_PATH_MICRODADOS = "/tmp/data/microdados/output/"

    OUTPUT_PATH_BRASIL = "/tmp/data/BRASIL/output/"

    OUTPUT_PATH_UF = "/tmp/data/UF/output/"

    OUTPUT_PATH_MUNICIPIO = "/tmp/data/MUNICIPIO/output/"
    
