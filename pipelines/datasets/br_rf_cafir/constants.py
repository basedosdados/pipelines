# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""


from enum import Enum


class constants(Enum):
    URL = ["https://arquivos.receitafederal.gov.br/cafir/"]

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.8,en-US;q=0.5,en;q=0.3",
        "Sec-GPC": "1",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Priority": "u=0, i",
    }

    PATH = [
        "/tmp/input/br_rf_cafir",
        "/tmp/output/br_rf_cafir",
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
