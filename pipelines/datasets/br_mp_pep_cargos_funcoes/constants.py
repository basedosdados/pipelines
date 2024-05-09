# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_mp_pep_cargos_funcoes project
    """

    TARGET = "http://painel.pep.planejamento.gov.br/QvAJAXZfc/opendoc.htm?document=painelpep.qvw&lang=en-US&host=Local&anonymous=true"

    PATH = "/tmp/br_mp_pep_cargos_funcoes/"

    INPUT_DIR = "/tmp/br_mp_pep_cargos_funcoes/input"

    OUTPUT_DIR = "/tmp/br_mp_pep_cargos_funcoes/output"

    XPATHS = {
        # Cargos e Funções
        "card_home_funcoes": "/html/body/div[5]/div/div[88]",
        # Aba Tabelas
        "tabelas": "/html/body/div[5]/div/div[280]/div[3]"
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

    TABLE_ARCHITECHTURE_URL = "https://docs.google.com/spreadsheets/d/1f_fecSsP3s5HO1pJk79ydaNb63Kj5ohrVkc48twBdsI/edit#gid=0"

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
        "Nome Cor Origem Etnica": "raca_cor",
        "CCE & FCE": "cce_e_fce",
        "DAS e correlatas": "das_e_correlatas",
    }
