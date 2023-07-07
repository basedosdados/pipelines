# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""


from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_ms_cnes project
    """

    PATH = [
        "/tmp/br_ms_cnes/input/",
        "/tmp/br_ms_cnes/output/",
    ]

    TABLE = ["estabelecimento", "profissionais"]

    DATABASE_GROUPS = {
        "CNES": ["ST", "PF"],
    }

    UF = [
        "MA",
        "SP",
        "ES",
        "MG",
        "PR",
        "SC",
        "RS",
        "MS",
        "GO",
        "AC",
        "AL",
        "AP",
        "AM",
        "BA",
        "CE",
        "DF",
        "MA",
        "MT",
        "PA",
        "PB",
        "PE",
        "PI",
        "RN",
        "RO",
        "RR",
        "SE",
        "TO",
    ]

    MESES = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"]
