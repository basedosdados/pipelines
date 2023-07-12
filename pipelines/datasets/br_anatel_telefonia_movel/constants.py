# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_anatel_telefonia_movel project
    """

    ORDEM = [
        "ano",
        "mes",
        "sigla_uf",
        "ddd",
        "id_municipio",
        "cnpj",
        "empresa",
        "porte_empresa",
        "tecnologia",
        "sinal",
        "modalidade",
        "pessoa",
        "produto",
        "acessos",
    ]

    RENAME = {
        "Ano": "ano",
        "Mês": "mes",
        "Grupo Econômico": "grupo_economico",
        "Empresa": "empresa",
        "CNPJ": "cnpj",
        "Porte da Prestadora": "porte_empresa",
        "UF": "sigla_uf",
        "Município": "municipio",
        "Código IBGE Município": "id_municipio",
        "Código Nacional": "ddd",
        "Código Nacional (Chip)": "ddd_chip",
        "Modalidade de Cobrança": "modalidade",
        "Tecnologia": "tecnologia",
        "Tecnologia Geração": "sinal",
        "Tipo de Pessoa": "pessoa",
        "Tipo de Produto": "produto",
        "Acessos": "acessos",
    }

    URL = "https://www.anatel.gov.br/dadosabertos/paineis_de_dados/acessos/acessos_telefonia_movel.zip"

    INPUT_PATH = "/tmp/data/input/"

    OUTPUT_PATH_MICRODADOS = "/tmp/data/microdados/output/"

    OUTPUT_PATH_BRASIL = "/tmp/data/BRASIL/output/"

    OUTPUT_PATH_UF = "/tmp/data/UF/output/"

    OUTPUT_PATH_MUNICIPIO = "/tmp/data/MUNICIPIO/output/"
