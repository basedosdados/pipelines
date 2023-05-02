# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""


###############################################################################
#
# Esse é um arquivo onde podem ser declaratas constantes que serão usadas
# pelo projeto br_bcb_agencia.
#
# Por ser um arquivo opcional, pode ser removido sem prejuízo ao funcionamento
# do projeto, caos não esteja em uso.
#
# Para declarar constantes, basta fazer conforme o exemplo abaixo:
#
# ```
# class constants(Enum):
#     """
#     Constant values for the br_bcb_agencia project
#     """
#     FOO = "bar"
# ```
#
# Para usá-las, basta fazer conforme o exemplo abaixo:
#
# ```py
# from pipelines.datasets.br_bcb_agencia.constants import constants
# print(constants.FOO.value)
# ```
#
###############################################################################

from enum import Enum


class constants(Enum):  # pylint: disable=c0103

    """
    Constants for utils.
    """

    AGENCIA_URL = "https://www.bcb.gov.br/fis/info/agencias.asp?frame=1"
    AGENCIA_XPATH = "/html/body/div/table/tbody/tr[2]/td[2]/form/select"
    DOWNLOAD_PATH_AGENCIA = "/tmp/input/"
    CLEANED_FILES_PATH_AGENCIA = "/tmp/output/"
