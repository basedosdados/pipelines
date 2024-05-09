# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""

###############################################################################


from enum import Enum


class constants(Enum):  # pylint: disable=c0103

    URL = "https://dados.gov.br/dados/conjuntos-dados/acessos---banda-larga-fixa"

    INPUT_PATH = "/tmp/data/input/"


    TABLES_OUTPUT_PATH = {
        'microdados' : "/tmp/data/microdados/output/",
        'densidade_brasil' : "/tmp/data/BRASIL/output/",
        'densidade_uf' : "/tmp/data/UF/output/",
        'densidade_municipio' : "/tmp/data/MUNICIPIO/output/"
        }

    # ! ------------> Microdados
    RENAME_MICRODADOS = {
            "Ano": "ano",
            "Mês": "mes",
            "Grupo Econômico": "grupo_economico",
            "Empresa": "empresa",
            "CNPJ": "cnpj",
            "Porte da Prestadora": "porte_empresa",
            "UF": "sigla_uf",
            "Município": "municipio",
            "Código IBGE Município": "id_municipio",
            "Faixa de Velocidade": "velocidade",
            "Tecnologia": "tecnologia",
            "Meio de Acesso": "transmissao",
            "Acessos": "acessos",
            "Tipo de Pessoa": "pessoa",
            "Tipo de Produto": "produto",
        }

    ORDER_COLUMNS_MICRODADOS = [
            "ano",
            "mes",
            "sigla_uf",
            "id_municipio",
            "cnpj",
            "empresa",
            "porte_empresa",
            "tecnologia",
            "transmissao",
            "velocidade",
            "produto",
            "acessos",
        ]

    SORT_VALUES_MICRODADOS = [
            "ano",
            "mes",
            "sigla_uf",
            "id_municipio",
            "cnpj",
            "empresa",
            "porte_empresa",
            "tecnologia",
            "transmissao",
            "velocidade",
        ]

    DROP_COLUMNS_MICRODADOS = [
        "grupo_economico",
        "municipio"
        ]

    # ! ------------ > Densidade Brasil

    RENAME_COLUMNS_BRASIL = {
                "Ano": "ano",
                "Mês": "mes",
                "Densidade": "densidade"
    }

    DROP_COLUMNS_BRASIL = ["UF", "Município", "Geografia", "Código IBGE"]

    # ! --------------> Densidade UF

    RENAME_COLUMNS_UF = {
            "Ano": "ano",
            "Mês": "mes",
            "UF": "sigla_uf",
            "Densidade": "densidade",
        }

    DROP_COLUMNS_UF = [
        "Município",
        "Código IBGE",
        "Geografia"
        ]

    # ! -----------------> Densidade Municipio

    RENAME_COLUMNS_MUNICIPIO = {
            "Ano": "ano",
            "Mês": "mes",
            "UF": "sigla_uf",
            "Código IBGE": "id_municipio",
            "Densidade": "densidade",
        }

    DROP_COLUMNS_MUNICIPIO = ["Município", "Geografia"]