# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""


###############################################################################
#
# Esse é um arquivo onde podem ser declaratas constantes que serão usadas
# pelo projeto br_mp_pep_cargos_funcoes.
#
# Por ser um arquivo opcional, pode ser removido sem prejuízo ao funcionamento
# do projeto, caos não esteja em uso.
#
# Para declarar constantes, basta fazer conforme o exemplo abaixo:
#
# ```
# class constants(Enum):
#     """
#     Constant values for the br_mp_pep_cargos_funcoes project
#     """
#     FOO = "bar"
# ```
#
# Para usá-las, basta fazer conforme o exemplo abaixo:
#
# ```py
# from pipelines.datasets.br_mp_pep_cargos_funcoes.constants import constants
# print(constants.FOO.value)
# ```
#
###############################################################################

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_mp_pep_cargos_funcoes project
    """

    TARGET = "http://painel.pep.planejamento.gov.br/QvAJAXZfc/opendoc.htm?document=painelpep.qvw&lang=en-US&host=Local&anonymous=true"

    PATH = "/tmp/br_mp_pep_cargos_funcoes/"

    TMP_DATA_DIR = "/tmp/br_mp_pep_cargos_funcoes/tmp"

    INPUT_DIR = "/tmp/br_mp_pep_cargos_funcoes/input"

    OUTPUT_DIR = "/tmp/br_mp_pep_cargos_funcoes/output"

    XPATHS = {
        # Cargos e Funções
        "card_home_funcoes": "/html/body/div[5]/div/div[88]",
        # Aba Tabelas
        "tabelas": "/html/body/div[5]/div/div[280]/div[3]/table/tbody/tr/td",
    }

    SELECTIONS_DIMENSIONS = [
        "Mês Cargos",
        "Natureza Juridica",
        "Orgão Superior",
        "Orgão",
        "Tipo de Função Detalhada",
        "Função",
        "Nível Função",
        "Subnível Função",
        "Região",
        "UF",
        "Nome Cor Origem Etnica",
        "Faixa Etária",
        "Escolaridade do Servidor",
        "Sexo",
    ]

    SELECTIONS_METRICS = ["CCE & FCE", "DAS e correlatas"]

    CHROME_DRIVER = "https://chromedriver.storage.googleapis.com/114.0.5735.90/chromedriver_linux64.zip"

    RENAMES = {
        "Ano": "ano",
        "Mês": "mes",
        "Função": "funcao",
        "Natureza Juridica": "natureza_juridica",
        "Orgão Superior": "orgao_superior",
        "Escolaridade do Servidor": "escolaridade_servidor",
        "Orgão": "orgao",
        "Região": "regiao",
        "Sexo": "sexo",
        "Nível Função": "nivel_funcao",
        "Subnível Função": "subnivel_funcao",
        "UF": "sigla_uf",
        "Faixa Etária": "faixa_etaria",
        "Nome Cor Origem Etnica": "nome_cor_origem_etnica",
        "CCE & FCE": "cce_e_fce",
        "DAS e correlatas": "das_e_correlatas",
    }
