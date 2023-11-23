# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""

###############################################################################
import datetime
from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the mundo_transfermarkt_competicoes_internacionais project
    """

    SEASON = datetime.datetime.now().year - 1

    ORDEM_COLUNA_FINAL = [
        "temporada",
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
        "idade_tecnico_man",
        "idade_tecnico_vis",
        "data_inicio_tecnico_man",
        "data_inicio_tecnico_vis",
        "data_final_tecnico_man",
        "data_final_tecnico_vis",
        "proporcao_sucesso_man",
        "proporcao_sucesso_vis",
        "valor_equipe_titular_man",
        "valor_equipe_titular_vis",
        "valor_medio_equipe_titular_man",
        "valor_medio_equipe_titular_vis",
        "convocacao_selecao_principal_man",
        "convocacao_selecao_principal_vis",
        "selecao_juniores_man",
        "selecao_juniores_vis",
        "estrangeiros_man",
        "estrangeiros_vis",
        "socios_man",
        "socios_vis",
        "idade_media_titular_man",
        "idade_media_titular_vis",
        "gols_man",
        "gols_vis",
        "prorrogacao",
        "penalti",
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
