# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""


###############################################################################
#
# Esse é um arquivo onde podem ser declaratas constantes que serão usadas
# pelo projeto br_rj_isp_estatisticas_seguranca.
#
# Por ser um arquivo opcional, pode ser removido sem prejuízo ao funcionamento
# do projeto, caos não esteja em uso.
#
# Para declarar constantes, basta fazer conforme o exemplo abaixo:
#
# ```
# class constants(Enum):
#     """
#     Constant values for the br_isp_estatisticas_seguranca project
#     """
#     FOO = "bar"
# ```
#
# Para usá-las, basta fazer conforme o exemplo abaixo:
#
# ```py
# from pipelines.datasets.br_isp_estatisticas_seguranca.constants import constants
# print(constants.FOO.value)
# ```
#
###############################################################################

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_isp_estatisticas_seguranca project
    """

    # datasets raw name

    EVOLUCAO_MENSAL_CISP = "BaseDPEvolucaoMensalCisp.csv"
    EVOLUCAO_MENSAL_UF = "DOMensalEstadoDesde1991.csv"
    TAXA_EVOLUCAO_MENSAL_UF = "BaseEstadoTaxaMes.csv"
    EVOLUCAO_MENSAL_MUNICIPIO = "BaseMunicipioMensal.csv"
    TAXA_EVOLUCAO_MENSAL_MUNICIPIO = "BaseMunicipioTaxaMes.csv"
    ARMAS_APREENDIDADAS_MENSAL = "ArmasDP2003_2006.csv"
    EVOLUCAO_POLICIAL_MORTO = "PoliciaisMortos.csv"
    FEMINICIDIO_MENSAL_CISP = "BaseFeminicidioEvolucaoMensalCisp.csv"

    # paths
    INPUT_PATH = "tmp/input/"
    OUTPUT_PATH = "tmp/output/"

    # urls =
    URL = "http://www.ispdados.rj.gov.br/"
