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
