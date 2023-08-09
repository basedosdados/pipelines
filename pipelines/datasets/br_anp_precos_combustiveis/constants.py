# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    RENAME = {
        "Revenda": "nome_estabelecimento",
        "CNPJ da Revenda": "cnpj_revenda",
        "Bairro": "bairro_revenda",
        "Cep": "cep_revenda",
        "Produto": "produto",
        "Valor de Venda": "preco_venda",
        "Valor de Compra": "preco_compra",
        "Unidade de Medida": "unidade_medida",
        "Bandeira": "bandeira_revenda",
        "Estado - Sigla": "sigla_uf",
        "Municipio": "nome",
        "Data da Coleta": "data_coleta",
        "Nome da Rua": "nome_rua",
        "Numero Rua": "numero_rua",
        "Complemento": "complemento",
    }

    ORDEM = [
        "ano",
        "sigla_uf",
        "id_municipio",
        "bairro_revenda",
        "cep_revenda",
        "endereco_revenda",
        "cnpj_revenda",
        "nome_estabelecimento",
        "bandeira_revenda",
        "data_coleta",
        "produto",
        "unidade_medida",
        "preco_compra",
        "preco_venda",
    ]

    URLS = [
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/ultimas-4-semanas-glp.csv",
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/ultimas-4-semanas-gasolina-etanol.csv",
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/ultimas-4-semanas-diesel-gnv.csv",
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsan/2023/precos-glp-07.csv"
    ]

    PATH_INPUT = "/tmp/input/"

    PATH_OUTPUT = "/tmp/output/"

    URL_DIESEL_GNV = "/tmp/input/ultimas-4-semanas-glp.csv"
    URL_GASOLINA_ETANOL = "/tmp/input/ultimas-4-semanas-gasolina-etanol.csv"
    URL_GLP = "/tmp/input/ultimas-4-semanas-diesel-gnv.csv"
    TEMPORARIO = "/tmp/input/precos-glp-07.csv"
