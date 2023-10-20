# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""
from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    STF_INPUT = "/tmp/input/"
    STF_OUTPUT = "/tmp/output/"

    STF_LINK = "https://transparencia.stf.jus.br/extensions/decisoes/decisoes.html"

    RENAME = {
        "Ano da decisão": "ano",
        "Classe": "classe",
        "Número": "numero",
        "Nome Ministro(a)": "relator",
        "Processo": "link",
        "Subgrupo andamento decisão": "subgrupo_andamento",
        "Andamento decisão": "andamento",
        "Observação do andamento": "observacao_andamento_decisao",
        "Indicador virtual": "modalidade_julgamento",
        "Indicador colegiada": "tipo_julgamento",
        "Indicador eletrônico": "meio_tramitacao",
        "Indicador de tramitação": "indicador_tramitacao",
        "Assuntos do processo": "assunto_processo",
        "Ramo direito": "ramo_direito",
        "Data de autuação": "data_autuacao",
        "Data da decisão": "data_decisao",
        "Data baixa": "data_baixa_processo",
    }

    ORDEM = [
        "ano",
        "classe",
        "numero",
        "relator",
        "link",
        "subgrupo_andamento",
        "andamento",
        "observacao_andamento_decisao",
        "modalidade_julgamento",
        "tipo_julgamento",
        "meio_tramitacao",
        "indicador_tramitacao",
        "assunto_processo",
        "ramo_direito",
        "data_autuacao",
        "data_decisao",
        "data_baixa_processo",
    ]
