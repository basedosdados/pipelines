# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_ons_avaliacao_operacao project
    """

    PATH = "/tmp/br_ons_avaliacao_operacao/"

    TABLE_NAME_LIST = [
        "reservatorio",
        "geracao_usina",
        "geracao_termica_motivo_despacho",
        "energia_natural_afluente",
        "energia_armazenada_reservatorio",
    ]

    TABLE_NAME_URL_DICT = {
        "reservatorio": "https://dados.ons.org.br/dataset/reservatorio",
        "geracao_usina": "https://dados.ons.org.br/dataset/geracao-usina-2",
        "geracao_termica_motivo_despacho": "https://dados.ons.org.br/dataset/geracao-termica-despacho-2",
        "energia_natural_afluente": "https://dados.ons.org.br/dataset/ena-diario-por-reservatorio",
        "energia_armazenada_reservatorio": "https://dados.ons.org.br/dataset/ear-diario-por-reservatorio",
    }

    TABLE_NAME_ARCHITECHTURE_DICT = {
        "reservatorio": "https://docs.google.com/spreadsheets/d/1uxyZUEgqPy66hSlpe7i9vvbQ7XRZIjZj/edit#gid=2021213626",
        "geracao_usina": "https://docs.google.com/spreadsheets/d/1tXcbsZSysadZwBNBvr4_-m0-3I0tVjNU/edit#gid=390580706",
        "geracao_termica_motivo_despacho": "https://docs.google.com/spreadsheets/d/1ovWTxHMRb5NdDdS9_krBeoJvsRDAe4xp/edit#gid=838247806",
        "energia_natural_afluente": "https://docs.google.com/spreadsheets/d/1U5NFfYaaB15ckexd76eSFI1Er48qOG-o/edit#gid=142578613",
        "energia_armazenada_reservatorio": "https://docs.google.com/spreadsheets/d/1zXP_ByoNsH-XTaTbIZjtDkbmsdD3kbNn/edit#gid=337446516",
    }
