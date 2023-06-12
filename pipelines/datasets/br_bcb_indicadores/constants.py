# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""


###############################################################################
#
# Esse é um arquivo onde podem ser declaratas constantes que serão usadas
# pelo projeto br_bcb_indicadores.
#
# Por ser um arquivo opcional, pode ser removido sem prejuízo ao funcionamento
# do projeto, caos não esteja em uso.
#
# Para declarar constantes, basta fazer conforme o exemplo abaixo:
#
# ```
# class constants(Enum):
#     """
#     Constant values for the br_bcb_indicadores project
#     """
#     FOO = "bar"
# ```
#
# Para usá-las, basta fazer conforme o exemplo abaixo:
#
# ```py
# from pipelines.datasets.br_bcb_indicadores.constants import constants
# print(constants.FOO.value)
# ```
#
###############################################################################

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_bcb_indicadores project
    """

    URL_BCB = {
        "taxa_cambio": "https://docs.google.com/spreadsheets/d/1EHjZmmPMlGxD0ACORxH-8Yhh3bcs77Y_yWMFhOWnvlc/edit#gid=0"  # Info of treatment for the table
    }
