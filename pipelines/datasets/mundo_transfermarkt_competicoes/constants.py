# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""
###############################################################################

import datetime
from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the mundo_transfermarkt project
    """

    DATA_ATUAL = datetime.datetime.now().strftime("%Y-%m-%d")
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
        "time_man",
        "time_vis",
        "tecnico_man",
        "tecnico_vis",
        "colocacao_man",
        "colocacao_vis",
        "valor_equipe_titular_man",
        "valor_equipe_titular_vis",
        "idade_media_titular_man",
        "idade_media_titular_vis",
        "gols_man",
        "gols_vis",
        "gols_1_tempo_man",
        "gols_1_tempo_vis",
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

    ORDEM_COPA_BRASIL = [
        "ano_campeonato",
        "data",
        "horario",
        "fase",
        "tipo_fase",
        "estadio",
        "arbitro",
        "publico",
        "publico_max",
        "time_man",
        "time_vis",
        "tecnico_man",
        "tecnico_vis",
        "valor_equipe_titular_man",
        "valor_equipe_titular_vis",
        "idade_media_titular_man",
        "idade_media_titular_vis",
        "gols_man",
        "gols_vis",
        "gols_1_tempo_man",
        "gols_1_tempo_vis",
        "penalti",
        "gols_penalti_man",
        "gols_penalti_vis",
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
