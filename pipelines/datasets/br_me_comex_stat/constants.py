# -*- coding: utf-8 -*-
"""
Constants for br_me_comex_stat
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constants for br_me_comex_stat
    """

    PATH = "/tmp/br_me_comex_stat/"

    TABLE_TYPE = ["mun", "ncm"]

    TABLE_MUNICIPIO = [
        "EXP_COMPLETA_MUN",
        "IMP_COMPLETA_MUN",
    ]

    TABLE_NCM = [
        "EXP_COMPLETA",
        "IMP_COMPLETA",
    ]

    TABLE_NAME = [
        "mun_imp",
        "mun_exp",
        "ncm_imp",
        "ncm_exp",
    ]
