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
        "microdados_governo_federal" : {
            "INPUT_DATA" : "/tmp/input/microdados_governo_federal",
            "OUTPUT_DATA" : "/tmp/output/microdados_governo_federal",
            "URL" : "https://portaldatransparencia.gov.br/download-de-dados/cpgf/",
            "READ" : "_CPGF",
            "ONLY_ONE_FILE" : False},

        "microdados_compras_centralizadas" : {
            "INPUT_DATA" : "/tmp/input/microdados_compras_centralizadas",
            "OUTPUT_DATA" : "/tmp/output/microdados_compras_centralizadas",
            "URL" : "https://portaldatransparencia.gov.br/download-de-dados/cpcc/",
            "READ" : "_CPGFComprasCentralizadas",
            "ONLY_ONE_FILE" : False},

        "microdados_defesa_civil" : {
            "INPUT_DATA" : "/tmp/input/microdados_defesa_civil",
            "OUTPUT_DATA" : "/tmp/output/microdados_defesa_civil",
            "URL" : "https://portaldatransparencia.gov.br/download-de-dados/cpdc/",
            "READ" : "_CPDC",
            "ONLY_ONE_FILE" : False}
        }

    # ! ============================================== CGU - Servidores Públicos do Executivo Federal ==============================================
    URL_SERVIDORES = "http://portaldatransparencia.gov.br/download-de-dados/servidores/"

    TABELA_SERVIDORES = {
        "afastamentos": {
            "NAME_TABLE": "_Afastamentos.csv",
            "ARCHITECTURE": "https://docs.google.com/spreadsheets/d/1NQ4t9l8znClnfM8NYBLBkI9PoWV5UAosUZ1KGvZe-T0/edit#gid=0",
            "READ": {
                "Servidores_BACEN": "BACEN",
                "Servidores_SIAPE": "SIAPE",
            },
            "ONLY_TABLE": True,
            "INPUT": "/tmp/input/cgu_servidores/afastamentos",
            "OUTPUT": "/tmp/output/cgu_servidores/afastamentos",
        },
        "cadastro_aposentados": {
            "ARCHITECTURE": "https://docs.google.com/spreadsheets/d/1_t_JsWbuGlg8cz_2RYYNMuulzA4RHydfJ4TA-wH9Ch8/edit#gid=0",
            "NAME_TABLE": "_Cadastro.csv",
            "READ": {
                "Aposentados_SIAPE": "SIAPE",
                "Aposentados_BACEN": "BACEN",
            },
            "ONLY_TABLE": True,
            "INPUT": "/tmp/input/cgu_servidores/cadastro_aposentados",
            "OUTPUT": "/tmp/output/cgu_servidores/cadastro_aposentados",
        },
        "observacoes": {
            "ARCHITECTURE": "https://docs.google.com/spreadsheets/d/1BWt6yvKTfNW0XCDNIsIu8NhSKjhbDVJjnEwvnEmVkRc/edit#gid=0",
            "NAME_TABLE": "_Observacoes.csv",
            "READ": {
                "Aposentados_BACEN": "Aposentados BACEN",
                "Aposentados_SIAPE": "Aposentados SIAPE",
                "Militares": "Militares",
                "Pensionistas_BACEN": "Pensionistas BACEN",
                "Pensionistas_DEFESA": "Pensionistas DEFESA",
                "Pensionistas_SIAPE": "Pensionistas SIAPE",
                "Reserva_Reforma_Militares": "Reserva Reforma Militares",
                "Servidores_BACEN": "Servidores BACEN",
                "Servidores_SIAPE": "Servidores SIAPE",
                "Militares": "Militares",
            },
            "ONLY_TABLE": True,
            "INPUT": "/tmp/input/cgu_servidores/observacoes",
            "OUTPUT": "/tmp/output/cgu_servidores/observacoes",
        },
        "cadastro_pensionistas": {
            "ARCHITECTURE": "https://docs.google.com/spreadsheets/d/1G_RPhSUZRrCqcQCP1WSjBiYbnjirqTp0yaLNUaPi_7U/edit#gid=0",
            "NAME_TABLE": "_Cadastro.csv",
            "READ": {
                "Pensionistas_SIAPE": "SIAPE",
                "Pensionistas_DEFESA": "Defesa",
                "Pensionistas_BACEN": "BACEN",
            },
            "ONLY_TABLE": True,
            "INPUT": "/tmp/input/cgu_servidores/cadastro_pensionistas",
            "OUTPUT": "/tmp/output/cgu_servidores/cadastro_pensionistas",
        },
        "remuneracao": {
            "ARCHITECTURE": "https://docs.google.com/spreadsheets/d/1LJ8_N53OoNEQQ1PMAeIPKq1asB29MUP5SRoFWnUI6Zg/edit#gid=0",
            "NAME_TABLE": "_Remuneracao.csv",
            "READ": {
                "Militares": "Militares",
                "Pensionistas_BACEN": "Pensionistas BACEN",
                "Pensionistas_DEFESA": "Pensionistas DEFESA",
                "Reserva_Reforma_Militares": "Reserva Reforma Militares",
                "Servidores_BACEN": "Servidores BACEN",
                "Servidores_SIAPE": "Servidores SIAPE",
            },
            "ONLY_TABLE": True,
            "INPUT": "/tmp/input/cgu_servidores/remuneracao",
            "OUTPUT": "/tmp/output/cgu_servidores/remuneracao",
        },
        "cadastro_reserva_reforma_militares": {
            "ARCHITECTURE": "https://docs.google.com/spreadsheets/d/1vqWjATWjHK-6tbj_ilwbhNWReqe3AKd3VTpzBbPu4qI/edit#gid=0",
            "NAME_TABLE": "_Cadastro.csv",
            "READ": {"Reserva_Reforma_Militares": "Reserva Reforma Militares"},
            "ONLY_TABLE": True,
            "INPUT": "/tmp/input/cgu_servidores/cadastro_reserva_reforma_militares",
            "OUTPUT": "/tmp/output/cgu_servidores/cadastro_reserva_reforma_militares",
        },
        "cadastro_servidores": {
            "ARCHITECTURE": "https://docs.google.com/spreadsheets/d/1U57P5XhCw9gERD8sN24P0vDK0CjkUuOQZwOoELdE3Jg/edit#gid=0",
            "NAME_TABLE": "_Cadastro.csv",
            "READ": {
                "Servidores_BACEN": "BACEN",
                "Servidores_SIAPE": "SIAPE",
                "Militares": "Militares",
            },
            "ONLY_TABLE": True,
            "INPUT": "/tmp/input/cgu_servidores/cadastro_servidores",
            "OUTPUT": "/tmp/output/cgu_servidores/cadastro_servidores",
        },
    }
