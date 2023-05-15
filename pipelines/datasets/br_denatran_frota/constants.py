# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""


###############################################################################
#
# Esse é um arquivo onde podem ser declaratas constantes que serão usadas
# pelo projeto br_denatran_frota.
#
# Por ser um arquivo opcional, pode ser removido sem prejuízo ao funcionamento
# do projeto, caos não esteja em uso.
#
# Para declarar constantes, basta fazer conforme o exemplo abaixo:
#
# ```
# class constants(Enum):
#     """
#     Constant values for the br_denatran_frota project
#     """
#     FOO = "bar"
# ```
#
# Para usá-las, basta fazer conforme o exemplo abaixo:
#
# ```py
# from pipelines.datasets.br_denatran_frota.constants import constants
# print(constants.FOO.value)
# ```
#
###############################################################################

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_denatran_frota project
    """

    # -*- coding: utf-8 -*-

    MONTHS = {
        "janeiro": 1,
        "fevereiro": 2,
        "marco": 3,
        "abril": 4,
        "maio": 5,
        "junho": 6,
        "julho": 7,
        "agosto": 8,
        "setembro": 9,
        "outubro": 10,
        "novembro": 11,
        "dezembro": 12,
    }

    DATASET = "br_denatran_frota"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
    }

    DICT_UFS = {
        "AC": "Acre",
        "AL": "Alagoas",
        "AP": "Amapá",
        "AM": "Amazonas",
        "BA": "Bahia",
        "CE": "Ceará",
        "DF": "Distrito Federal",
        "ES": "Espírito Santo",
        "GO": "Goiás",
        "MA": "Maranhão",
        "MT": "Mato Grosso",
        "MS": "Mato Grosso do Sul",
        "MG": "Minas Gerais",
        "PA": "Pará",
        "PB": "Paraíba",
        "PR": "Paraná",
        "PE": "Pernambuco",
        "PI": "Piauí",
        "RJ": "Rio de Janeiro",
        "RN": "Rio Grande do Norte",
        "RS": "Rio Grande do Sul",
        "RO": "Rondônia",
        "RR": "Roraima",
        "SC": "Santa Catarina",
        "SP": "São Paulo",
        "SE": "Sergipe",
        "TO": "Tocantins",
    }

    SUBSTITUTIONS = {
        ("RN", "assu"): "acu",
        ("PB", "sao domingos de pombal"): "sao domingos",
        ("PB", "santarem"): "joca claudino",
        ("SP", "embu"): "embu das artes",
        ("TO", "sao valerio da natividade"): "sao valerio",
    }

    DOWNLOAD_PATH = f"pipelines/datasets/{DATASET}/tmp/input"

    OUTPUT_PATH = f"pipelines/datasets/{DATASET}/tmp/output"

    UF_TIPO_BASIC_FILENAME = "frota_por_uf_e_tipo_de_veiculo"

    MUNIC_TIPO_BASIC_FILENAME = "frota_por_municipio_e_tipo"

    MONTHS_SHORT = {month[:3]: number for month, number in MONTHS.items()}

    UF_TIPO_HEADER = [
        "Grandes Regiões e\nUnidades da Federação",
        "TOTAL",
        "AUTOMÓVEL",
        "BONDE",
        "CAMINHÃO",
        "CAMINHÃO TRATOR",
        "CAMINHONETE",
        "CAMIONETA",
        "CHASSI PLATAFORMA",
        "CICLOMOTOR",
        "MICROÔNIBUS",
        "MOTOCICLETA",
        "MOTONETA",
        "ÔNIBUS",
        "QUADRICICLO",
        "REBOQUE",
        "SEMI-REBOQUE",
        "SIDE-CAR",
        "OUTROS",
        "TRATOR ESTEIRA",
        "TRATOR RODAS",
        "TRICICLO",
        "UTILITÁRIO",
    ]
