# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""


###############################################################################
#
# Esse é um arquivo onde podem ser declaratas constantes que serão usadas
# pelo projeto br_anatel_telefonia_movel.
#
# Por ser um arquivo opcional, pode ser removido sem prejuízo ao funcionamento
# do projeto, caos não esteja em uso.
#
# Para declarar constantes, basta fazer conforme o exemplo abaixo:
#
# ```
# class constants(Enum):
#     """
#     Constant values for the br_anatel_telefonia_movel project
#     """
#     FOO = "bar"
# ```
#
# Para usá-las, basta fazer conforme o exemplo abaixo:
#
# ```py
# from pipelines.datasets.br_anatel_telefonia_movel.constants import constants
# print(constants.FOO.value)
# ```
#
###############################################################################

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

    URL_TESTE = "https://www.anatel.gov.br/dadosabertos/paineis_de_dados/acessos/acessos_telefonia_movel.zip"
