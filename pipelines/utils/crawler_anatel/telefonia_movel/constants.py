# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects of the pipelines
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_anatel_telefonia_movel project
    """

    URL = "'https://dados.gov.br/dados/conjuntos-dados/acessos-autorizadas-smp'"

    INPUT_PATH = "/tmp/data/input/"

    TABLES_OUTPUT_PATH = {
        'microdados' : "/tmp/data/microdados/output/",
        'densidade_brasil' : "/tmp/data/BRASIL/output/",
        'densidade_uf' : "/tmp/data/UF/output/",
        'densidade_municipio' : "/tmp/data/MUNICIPIO/output/"
        }

    # ? MICRODADOS

    ORDER_COLUMNS_MICRODADOS = [
        "ano",
        "mes",
        "sigla_uf",
        "ddd",
        "id_municipio",
        "cnpj",
        "empresa",
        "porte_empresa",
        "tecnologia",
        "sinal",
        "modalidade",
        "pessoa",
        "produto",
        "acessos",
    ]

    RENAME_COLUMNS_MICRODADOS = {
        "Ano": "ano",
        "Mês": "mes",
        "Grupo Econômico": "grupo_economico",
        "Empresa": "empresa",
        "CNPJ": "cnpj",
        "Porte da Prestadora": "porte_empresa",
        "UF": "sigla_uf",
        "Município": "municipio",
        "Código IBGE Município": "id_municipio",
        "Código Nacional": "ddd",
        "Código Nacional (Chip)": "ddd_chip",
        "Modalidade de Cobrança": "modalidade",
        "Tecnologia": "tecnologia",
        "Tecnologia Geração": "sinal",
        "Tipo de Pessoa": "pessoa",
        "Tipo de Produto": "produto",
        "Acessos": "acessos",
    }

    DROP_COLUMNS_MICRODADOS = ["grupo_economico",
                            "municipio",
                            "ddd_chip"]

    # ? --------------------------------------------- > Densidade Municipio

    ORDER_COLUMNS_MUNICIPIO = ["Ano",
                            "Mês",
                            "UF",
                            "Código IBGE",
                            "Densidade"]

    RENAME_COLUMNS_MUNICIPIO = {
            "Ano": "ano",
            "Mês": "mes",
            "UF": "sigla_uf",
            "Código IBGE Município": "id_municipio",
            "Densidade": "densidade",
        }

    # ? --------------------------------------------- > Densidade UF

    RENAME_COLUMNS_UF = {
        "Ano": "ano",
        "Mês": "mes",
        "UF": "sigla_uf",
        "Densidade": "densidade"}

    ORDER_COLUMNS_UF = [
        "Ano",
        "Mês",
        "UF",
        "Densidade"
        ]

    # ? --------------------------------------------- > Densidade Brasil

    RENAME_COLUMNS_BRASIL = {"Ano": "ano", "Mês": "mes", "Densidade": "densidade"}

    ORDER_COLUMNS_BRASIL = ["Ano", "Mês", "Densidade"]