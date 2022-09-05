"""
Constants for br_me_comex_stat
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constants for br_me_comex_stat
    """

    UF = [
        "AC",
        "AL",
        "AM",
        "AP",
        "BA",
        "CE",
        "DF",
        "ES",
        "GO",
        "MA",
        "MG",
        "MS",
        "MT",
        "PA",
        "PB",
        "PE",
        "PI",
        "PR",
        "RJ",
        "RN",
        "RO",
        "RR",
        "SC",
        "SE",
        "RS",
        "SP",
        "TO",
    ]
    PATH = "/tmp/br_me_comex_stat/"
    TABLE = [
        "ncm_exportacao",
        "ncm_importacao",
        "municipio_exportacao",
        "municipio_importacao",
    ]
