# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""


###############################################################################
#
# Esse é um arquivo onde podem ser declaratas constantes que serão usadas
# pelo projeto mundo_transfermarkt.
#
# Por ser um arquivo opcional, pode ser removido sem prejuízo ao funcionamento
# do projeto, caos não esteja em uso.
#
# Para declarar constantes, basta fazer conforme o exemplo abaixo:
#
# ```
# class constants(Enum):
#     """
#     Constant values for the mundo_transfermarkt_competicoes project
#     """
#     FOO = "bar"
# ```
#
# Para usá-las, basta fazer conforme o exemplo abaixo:
#
# ```py
# from pipelines.datasets.mundo_transfermarkt_competicoes.constants import constants
# print(constants.FOO.value)
# ```
#
###############################################################################

from enum import Enum
import datetime


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the mundo_transfermarkt project
    """

    DATA_ATUAL_ANO = datetime.datetime.now().year
    SEASON = datetime.datetime.now().year - 1
    ORDEM_COLUNA_FINAL = [
        "ano_campeonato",
        "data",
        "horario",
        "rodada",
        "estadio",
        "arbitro",
        "publico",
        "publico_max",
        "valor_equipe_titular_man",
        "valor_equipe_titular_vis",
        "time_man",
        "time_vis",
        "tecnico_man",
        "tecnico_vis",
        "colocacao_man",
        "colocacao_vis",
        "gols_man",
        "gols_vis",
        "gols_1_tempo_man",
        "gols_1_tempo_vis",
        "idade_media_titular_man",
        "idade_media_titular_vis",
        "escanteios_man",
        "escanteios_vis",
        "faltas_man",
        "faltas_vis",
        "chutes_bola_parada_man",
        "chutes_bola_parada_vis",
        "defesas_man",
        "defesas_vis",
        "impedimentos_man",
        "impedimentos_vis",
        "chutes_man",
        "chutes_vis",
        "chutes_fora_man",
        "chutes_fora_vis",
    ]
