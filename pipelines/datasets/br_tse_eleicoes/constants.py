# -*- coding: utf-8 -*-
"""
Constants for br_tse_eleicoes pipeline.
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constants for utils.
    """
    
    CANDIDATOS22_ZIP="https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_2022.zip"
    BENS22_ZIP="https://cdn.tse.jus.br/estatistica/sead/odsele/bem_candidato/bem_candidato_2022.zip"
    DESPESA22_ZIP="https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas/prestacao_de_contas_eleitorais_candidatos_2018.zip"