# -*- coding: utf-8 -*-
"""
Constants for br_tse_eleicoes pipeline.
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constants for utils.
    """

    QUERY_MUNIPIPIOS = "select id_municipio, id_municipio_tse from `basedosdados.br_bd_diretorios_brasil.municipio`"
