# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_cgu_cartao_pagamento project
    """

    TABELA = {
        "microdados" : {
            "INPUT" : "/tmp/input/microdados",
            "OUTPUT" : "/tmp/output/microdados",
            "URL" : "https://portaldatransparencia.gov.br/download-de-dados/cpgf/",
            "READ" : "_CPGF",
            "UNICO" : False},

        "compras_centralizadas" : {
            "INPUT" : "/tmp/input/compras_centralizadas",
            "OUTPUT" : "/tmp/output/compras_centralizadas",
            "URL" : "https://portaldatransparencia.gov.br/download-de-dados/cpcc/",
            "READ" : "_CPGFComprasCentralizadas",
            "UNICO" : False},

        "defesa_civil" : {
            "INPUT" : "/tmp/input/defesa_civil",
            "OUTPUT" : "/tmp/output/defesa_civil",
            "URL" : "https://portaldatransparencia.gov.br/download-de-dados/cpdc/",
            "READ" : "_CPDC",
            "UNICO" : False}
        }