# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""

from enum import Enum
from datetime import datetime

class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_cgu_cartao_pagamento project
    """

    TABELA = {
        "microdados_governo_federal" : {
            "INPUT_DATA" : "/tmp/input/microdados_governo_federal",
            "OUTPUT_DATA" : "/tmp/output/microdados_governo_federal",
            "URL" : "https://portaldatransparencia.gov.br/download-de-dados/cpgf/",
            "READ" : "_CPGF",
            "UNICO" : False},

        "microdados_compras_centralizadas" : {
            "INPUT_DATA" : "/tmp/input/microdados_compras_centralizadas",
            "OUTPUT_DATA" : "/tmp/output/microdados_compras_centralizadas",
            "URL" : "https://portaldatransparencia.gov.br/download-de-dados/cpcc/",
            "READ" : "_CPGFComprasCentralizadas",
            "UNICO" : False},

        "microdados_defesa_civil" : {
            "INPUT_DATA" : "/tmp/input/microdados_defesa_civil",
            "OUTPUT_DATA" : "/tmp/output/microdados_defesa_civil",
            "URL" : "https://portaldatransparencia.gov.br/download-de-dados/cpdc/",
            "READ" : "_CPDC",
            "UNICO" : False}
        }

    year = datetime.now().year
    month = datetime.now().month