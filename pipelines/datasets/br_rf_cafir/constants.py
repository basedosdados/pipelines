# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""


from enum import Enum


class constants(Enum):
    URL = ["https://dadosabertos.rfb.gov.br/CAFIR/"]

    PATH = [
        "/tmp/br_rf_cafir/input/",
        "/tmp/br_rf_cafir/output/",
    ]

    TABLE = ["imoveis_rurais"]

    COLUMN_NAMES = [
        "id_imovel_incra",
        "area",
        "id_imovel_receita_federal",
        "nome",
        "situacao",
        "endereco",
        "zona_redefinir",
        "sigla_uf",
        "municipio",
        "cep",
        "data_inscricao",
        "status_rever",
        "cd_rever",
    ]

    WIDTHS = [8, 9, 13, 55, 2, 56, 40, 2, 40, 8, 8, 3, 1]

    DTYPE = {
        "id_imovel_incra": str,
        "area": str,
        "id_imovel_receita_federal": str,
        "nome": str,
        "situacao": str,
        "endereco": str,
        "zona_redefinir": str,
        "sigla_uf": str,
        "municipio": str,
        "cep": str,
        "data_inscricao": str,
        "status_rever": str,
        "cd_rever": str,
    }
