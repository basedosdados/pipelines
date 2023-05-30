# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_ons_avaliacao_operacao project
    """

    PATH = "/tmp/br_ons_estimativa_custos/"

    TABLE_NAME_LIST = [
        "custo_marginal_operacao_semi_horario",
        "custo_marginal_operacao_semanal",
        "balanco_energia_subsistemas",
        "balanco_energia_subsistemas_dessem",
    ]

    TABLE_NAME_URL_DICT = {
        "custo_marginal_operacao_semi_horario": "https://dados.ons.org.br/dataset/cmo-semi-horario",
        "custo_marginal_operacao_semanal": "https://dados.ons.org.br/dataset/cmo-semanal",
        "balanco_energia_subsistemas": "https://dados.ons.org.br/dataset/balanco-energia-subsistema",
        "balanco_energia_subsistemas_dessem": "https://dados.ons.org.br/dataset/balanco-energia-dessem",
    }

    TABLE_NAME_ARCHITECHTURE_DICT = {
        "custo_marginal_operacao_semi_horario": "https://docs.google.com/spreadsheets/d/1bOC18xdewzF_e75VxkmjfJrbBKEYAKEa/edit#gid=1512823303",
        "custo_marginal_operacao_semanal": "https://docs.google.com/spreadsheets/d/15wVtoiz6BJfmtRmRNjOxNxTBnvO2Ms-g/edit#gid=113107095",
        "balanco_energia_subsistemas": "https://docs.google.com/spreadsheets/d/1JukOONGlzRMqlVhBJ9CwRJw9nmMqo43r/edit#gid=1624318921",
        "balanco_energia_subsistemas_dessem": "https://docs.google.com/spreadsheets/d/1FjTRQ27Mkg0HOb7BqR915ZkfBoJ4KSl0/edit#gid=524444634",
    }
