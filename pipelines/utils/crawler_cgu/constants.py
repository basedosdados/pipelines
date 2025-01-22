# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""

from enum import Enum
import numpy as np

class constants(Enum):  # pylint: disable=c0103

    # ! ================================ CGU - Cartão de Pagamento ===========================================
    """
    Constant values for the br_cgu_cartao_pagamento project
    """

    TABELA = {
        "microdados_governo_federal" : {
            "INPUT" : "/tmp/input/microdados_governo_federal",
            "OUTPUT" : "/tmp/output/microdados_governo_federal",
            "URL" : "https://portaldatransparencia.gov.br/download-de-dados/cpgf/",
            "READ" : "_CPGF",
            "ONLY_ONE_FILE" : False},

        "microdados_compras_centralizadas" : {
            "INPUT" : "/tmp/input/microdados_compras_centralizadas",
            "OUTPUT" : "/tmp/output/microdados_compras_centralizadas",
            "URL" : "https://portaldatransparencia.gov.br/download-de-dados/cpcc/",
            "READ" : "_CPGFComprasCentralizadas",
            "ONLY_ONE_FILE" : False},

        "microdados_defesa_civil" : {
            "INPUT" : "/tmp/input/microdados_defesa_civil",
            "OUTPUT" : "/tmp/output/microdados_defesa_civil",
            "URL" : "https://portaldatransparencia.gov.br/download-de-dados/cpdc/",
            "READ" : "_CPDC",
            "ONLY_ONE_FILE" : False}
        }

    # ! ================================ CGU - Servidores Públicos do Executivo Federal ===========================================
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

    # ! ================================ CGU - Licitação e Contrato ===========================================

    TABELA_LICITACAO_CONTRATO = {
        "licitacao": {
            "URL": "https://portaldatransparencia.gov.br/download-de-dados/licitacoes/",
            "INPUT": "/tmp/input/cgu_licitacao_contrato/licitacao",
            "OUTPUT": "/tmp/output/cgu_licitacao_contrato/licitacao",
            "READ": "_Licitação.csv",
        },
        "licitacao_participante": {
            "URL": "https://portaldatransparencia.gov.br/download-de-dados/licitacoes/",
            "INPUT": "/tmp/input/cgu_licitacao_contrato/licitacao_participante",
            "OUTPUT": "/tmp/output/cgu_licitacao_contrato/licitacao_participante",
            "READ": "_ParticipantesLicitação.csv",
        },
        "licitacao_item": {
            "URL": "https://portaldatransparencia.gov.br/download-de-dados/licitacoes/",
            "INPUT": "/tmp/input/cgu_licitacao_contrato/licitacao_item",
            "OUTPUT": "/tmp/output/cgu_licitacao_contrato/licitacao_item",
            "READ": "_ItemLicitação.csv",
        },
        "licitacao_empenho": {
            "URL": "https://portaldatransparencia.gov.br/download-de-dados/licitacoes/",
            "INPUT": "/tmp/input/cgu_licitacao_contrato/licitacao_empenho",
            "OUTPUT": "/tmp/output/cgu_licitacao_contrato/licitacao_empenho",
            "READ": "_EmpenhosRelacionados.csv",
        },
        "contrato_compra": {
            "URL": "https://portaldatransparencia.gov.br/download-de-dados/compras/",
            "INPUT": "/tmp/input/cgu_licitacao_contrato/contrato_compra",
            "OUTPUT": "/tmp/output/cgu_licitacao_contrato/contrato_compra",
            "READ": "_Compras.csv",
        },
        "contrato_item": {
            "URL": "https://portaldatransparencia.gov.br/download-de-dados/compras/",
            "INPUT": "/tmp/input/cgu_licitacao_contrato/contrato_item",
            "OUTPUT": "/tmp/output/cgu_licitacao_contrato/contrato_item",
            "READ": "_ItemCompra.csv",
        },
        "contrato_termo_aditivo": {
            "URL": "https://portaldatransparencia.gov.br/download-de-dados/compras/",
            "INPUT": "/tmp/input/cgu_licitacao_contrato/contrato_termo_aditivo",
            "OUTPUT": "/tmp/output/cgu_licitacao_contrato/contrato_termo_aditivo",
            "READ": "_TermoAditivo.csv",
        },
    }

    TABELA_BENEFICIOS_CIDADAO = {
        "novo_bolsa_familia" : {
            "INPUT"  : "/tmp/input/novo_bolsa_familia/",
            "OUTPUT" : "/tmp/output/novo_bolsa_familia/",
            "URL" : "https://portaldatransparencia.gov.br/download-de-dados/novo-bolsa-familia/"
            },

        "bpc" : {
            "INPUT" : "/tmp/input/bpc/",
            "OUTPUT" : "/tmp/output/bpc/",
            "URL" : "https://portaldatransparencia.gov.br/download-de-dados/bpc/"
            },

        "garantia_safra" : {
            "INPUT" : "/tmp/input/garantia_safra/",
            "OUTPUT" : "/tmp/output/garantia_safra/",
            "URL" : "https://portaldatransparencia.gov.br/download-de-dados/garantia-safra/"}
        }

    DTYPES_NOVO_BOLSA_FAMILIA = {
        "MÊS COMPETÊNCIA": str,
        "MÊS REFERÊNCIA": str,
        "UF": str,
        "CÓDIGO MUNICÍPIO SIAFI": str,
        "NOME MUNICÍPIO": str,
        "CPF FAVORECIDO": str,
        "NIS FAVORECIDO": str,
        "NOME FAVORECIDO": str,
        "VALOR PARCELA": np.float64,
    }
    DTYPES_GARANTIA_SAFRA = {
        "MÊS REFERÊNCIA": str,
        "UF": str,
        "CÓDIGO MUNICÍPIO SIAFI": str,
        "NOME MUNICÍPIO": str,
        "NIS FAVORECIDO": str,
        "NOME FAVORECIDO": str,
        "VALOR PARCELA": np.float64,
    }

    DTYPES_BPC = {
        "MÊS COMPETÊNCIA": str,
        "MÊS REFERÊNCIA": str,
        "UF": str,
        "CÓDIGO MUNICÍPIO SIAFI": str,
        "NOME MUNICÍPIO": str,
        "NIS BENEFICIÁRIO": str,
        "CPF BENEFICIÁRIO": str,
        "NOME BENEFICIÁRIO": str,
        "NIS REPRESENTANTE LEGAL": str,
        "CPF REPRESENTANTE LEGAL": str,
        "NOME REPRESENTANTE LEGAL": str,
        "NÚMERO BENEFÍCIO": str,
        "BENEFÍCIO CONCEDIDO JUDICIALMENTE": str,
        "VALOR PARCELA": np.float64,
    }

    RENAMER_NOVO_BOLSA_FAMILIA = {
        "MÊS COMPETÊNCIA": "mes_competencia",
        "MÊS REFERÊNCIA": "mes_referencia",
        "UF": "sigla_uf",
        "CÓDIGO MUNICÍPIO SIAFI": "id_municipio_siafi",
        "NOME MUNICÍPIO": "municipio",
        "CPF FAVORECIDO": "cpf",
        "NIS FAVORECIDO": "nis",
        "NOME FAVORECIDO": "nome",
        "VALOR PARCELA": "valor",
    }
    RENAMER_GARANTIA_SAFRA = {
        "MÊS REFERÊNCIA": "mes_referencia",
        "UF": "sigla_uf",
        "CÓDIGO MUNICÍPIO SIAFI": "id_municipio_siafi",
        "NOME MUNICÍPIO": "municipio",
        "NIS FAVORECIDO": "nis",
        "NOME FAVORECIDO": "nome",
        "VALOR PARCELA": "valor",
    }
    RENAMER_BPC = {
        "MÊS COMPETÊNCIA": "mes_competencia",
        "MÊS REFERÊNCIA": "mes_referencia",
        "UF": "sigla_uf",
        "CÓDIGO MUNICÍPIO SIAFI": "id_municipio_siafi",
        "NOME MUNICÍPIO": "municipio",
        "NIS BENEFICIÁRIO": "nis",
        "CPF BENEFICIÁRIO": "cpf",
        "NOME BENEFICIÁRIO": "nome",
        "NIS REPRESENTANTE LEGAL": "nis_representante",
        "CPF REPRESENTANTE LEGAL": "cpf_representante",
        "NOME REPRESENTANTE LEGAL": "nome_representante",
        "NÚMERO BENEFÍCIO": "numero",
        "BENEFÍCIO CONCEDIDO JUDICIALMENTE": "concedido_judicialmente",
        "VALOR PARCELA": "valor",
    }

    DICT_FOR_TABLE = {
        "novo_bolsa_familia": {
            "dataset_id":"br_cgu_beneficios_cidadao",
            "table_id": "novo_bolsa_familia",
            "date_column_name": {"year": "ano_competencia", "month": "mes_competencia"},
            "date_format": "%Y-%m",
            "coverage_type": "part_bdpro",
            "time_delta": {"months": 6},
            "prefect_mode": "prod",
            "bq_project": "basedosdados"
        },
        "safra_garantia": {
            "dataset_id":"br_cgu_beneficios_cidadao",
            "table_id": "safra_garantia",
            "date_column_name": {"year": "ano_referencia", "month": "mes_referencia",
            },
            "date_format": "%Y-%m",
            "coverage_type": "part_bdpro",
            "time_delta": {"months": 6},
            "prefect_mode": "prod",
            "bq_project": "basedosdados"
        },
        "bpc": {
            "dataset_id":"br_cgu_beneficios_cidadao",
            "table_id": "bpc",
            "date_column_name": {"year": "ano_competencia", "month": "mes_competencia"},
            "date_format": "%Y-%m",
            "coverage_type": "part_bdpro",
            "time_delta": {"months": 6},
            "prefect_mode": "prod",
            "bq_project": "basedosdados",
        }
    }