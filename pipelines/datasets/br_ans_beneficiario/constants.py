# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""


from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_ans_beneficiario project
    """

    RAW_COLLUNS_TYPE = {
        "#ID_CMPT_MOVEL": str,
        "CD_OPERADORA": str,
        "NM_RAZAO_SOCIAL": str,
        "NR_CNPJ": str,
        "MODALIDADE_OPERADORA": str,
        "SG_UF": str,
        "CD_MUNICIPIO": str,
        "NM_MUNICIPIO": str,
        "TP_SEXO": str,
        "DE_FAIXA_ETARIA": str,
        "DE_FAIXA_ETARIA_REAJ": str,
        "CD_PLANO": str,
        "TP_VIGENCIA_PLANO": str,
        "DE_CONTRATACAO_PLANO": str,
        "DE_SEGMENTACAO_PLANO": str,
        "DE_ABRG_GEOGRAFICA_PLANO": str,
        "COBERTURA_ASSIST_PLAN": str,
        "TIPO_VINCULO": str,
        "QT_BENEFICIARIO_ATIVO": int,
        "QT_BENEFICIARIO_ADERIDO": int,
        "QT_BENEFICIARIO_CANCELADO": int,
        "QT_BENEFICIARIO_CANCELADO": int,
        "DT_CARGA": str,
    }
