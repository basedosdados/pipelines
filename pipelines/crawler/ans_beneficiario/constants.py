"""
Constant values for the datasets projects
"""

from enum import Enum


class constants(Enum):
    """
    Constant values for the br_ans_beneficiario project
    """

    # Colunas de texto são majoritariamente categóricas de baixa/média
    # cardinalidade (UF, sexo, faixa etária, modalidade, município, plano)
    # repetidas em milhões de linhas por arquivo. `category` deduplica os
    # valores em vez de guardar um `str` do Python por linha — corta bastante
    # o footprint do DataFrame em memória, sem mudar o valor persistido no
    # parquet (Arrow grava a string real via dicionário; BigQuery/dbt leem
    # como STRING normalmente). `#ID_CMPT_MOVEL` fica de fora porque não há
    # coluna com esse nome exato no CSV (o dtype nunca casa e o pandas infere
    # sozinho) — não mexemos nisso aqui.
    RAW_COLLUNS_TYPE = {
        "#ID_CMPT_MOVEL": str,
        "CD_OPERADORA": "category",
        "NM_RAZAO_SOCIAL": "category",
        "NR_CNPJ": "category",
        "MODALIDADE_OPERADORA": "category",
        "SG_UF": "category",
        "CD_MUNICIPIO": "category",
        "NM_MUNICIPIO": "category",
        "TP_SEXO": "category",
        "DE_FAIXA_ETARIA": "category",
        "DE_FAIXA_ETARIA_REAJ": "category",
        "CD_PLANO": "category",
        "TP_VIGENCIA_PLANO": "category",
        "DE_CONTRATACAO_PLANO": "category",
        "DE_SEGMENTACAO_PLANO": "category",
        "DE_ABRG_GEOGRAFICA_PLANO": "category",
        "COBERTURA_ASSIST_PLAN": "category",
        "TIPO_VINCULO": "category",
        "QT_BENEFICIARIO_ATIVO": int,
        "QT_BENEFICIARIO_ADERIDO": int,
        "QT_BENEFICIARIO_CANCELADO": int,
        "DT_CARGA": "category",
    }
