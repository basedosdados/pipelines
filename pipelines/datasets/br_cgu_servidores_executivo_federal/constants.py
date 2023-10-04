# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""

from enum import Enum

# fmt: off
__ALL_SHEETS__ = [
    "Aposentados_BACEN",
    "Aposentados_SIAPE",
    "Militares",
    "Pensionistas_BACEN",
    "Pensionistas_DEFESA",
    "Pensionistas_SIAPE",
    "Reserva_Reforma_Militares",
    "Servidores_BACEN",
    "Servidores_SIAPE",
]
# fmt: on


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_cgu_servidores_executivo_federal project
    """

    URL = "http://portaldatransparencia.gov.br/download-de-dados/servidores"

    # fmt: off
    SHEETS = __ALL_SHEETS__,
    # fmt: on

    TABLES = {
        "cadastro_aposentados": ["Aposentados_BACEN", "Aposentados_SIAPE"],
        "cadastro_pensionistas": [
            "Pensionistas_SIAPE",
            "Pensionistas_DEFESA",
            "Pensionistas_BACEN",
        ],
        "cadastro_servidores": ["Servidores_BACEN", "Servidores_SIAPE", "Militares"],
        "cadastro_reserva_reforma_militares": ["Reserva_Reforma_Militares"],
        "remuneracao": [
            "Militares",
            "Pensionistas_BACEN",
            "Pensionistas_DEFESA",
            "Reserva_Reforma_Militares",
            "Servidores_BACEN",
            "Servidores_SIAPE",
        ],
        "afastamentos": ["Servidores_BACEN", "Servidores_SIAPE"],
        "observacoes": __ALL_SHEETS__,
    }

    ARCH = {
        "afastamentos": "https://docs.google.com/spreadsheets/d/1NQ4t9l8znClnfM8NYBLBkI9PoWV5UAosUZ1KGvZe-T0/edit#gid=0",
        "cadastro_aposentados": "https://docs.google.com/spreadsheets/d/1_t_JsWbuGlg8cz_2RYYNMuulzA4RHydfJ4TA-wH9Ch8/edit#gid=0",
        "observacoes": "https://docs.google.com/spreadsheets/d/1BWt6yvKTfNW0XCDNIsIu8NhSKjhbDVJjnEwvnEmVkRc/edit#gid=0",
        "cadastro_pensionistas": "https://docs.google.com/spreadsheets/d/1G_RPhSUZRrCqcQCP1WSjBiYbnjirqTp0yaLNUaPi_7U/edit#gid=0",
        "remuneracao": "https://docs.google.com/spreadsheets/d/1LJ8_N53OoNEQQ1PMAeIPKq1asB29MUP5SRoFWnUI6Zg/edit#gid=0",
        "cadastro_reserva_reforma_militares": "https://docs.google.com/spreadsheets/d/1vqWjATWjHK-6tbj_ilwbhNWReqe3AKd3VTpzBbPu4qI/edit#gid=0",
        "cadastro_servidores": "https://docs.google.com/spreadsheets/d/1U57P5XhCw9gERD8sN24P0vDK0CjkUuOQZwOoELdE3Jg/edit#gid=0",
    }

    INPUT = "/tmp/input"

    OUTPUT = "/tmp/output"
