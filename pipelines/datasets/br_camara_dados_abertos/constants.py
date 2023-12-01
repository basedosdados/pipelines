# -*- coding: utf-8 -*-
from enum import Enum


class constants(Enum):
    TABLE_NAME_ARCHITECTURE = {
        "votacao_proposicao_afetada": "https://docs.google.com/spreadsheets/d/1sHBdS7dgGAWlegMs1rEy6OwUVDwTLcZy92pFX6U5rXM/edit#gid=0",
        "voto_parlamentar": "https://docs.google.com/spreadsheets/d/171Mykmg5qz54Kp35XgSD_IshngQHYE3jfGZ8yL-wEpE/edit#gid=0",
        "votacao_microdados": "https://docs.google.com/spreadsheets/d/1GZjzBqAQ5RqaB6kyOjD7iZeKSPxB07lAkZnvaGjREIY/edit#gid=0",
        "votacao_objeto": "https://docs.google.com/spreadsheets/d/1w2r5eK8jx3SwMTo83LKiI-wK_UE26SlTrJFqe_GZw4E/edit#gid=0",
        "votacao_orientacao_bancada": "https://docs.google.com/spreadsheets/d/1_gl5llaGw5Mr0A6Q8AUoHHiOiEGl7Ht8Yf3p2n4k8uA/edit#gid=0",
    }

    INPUT_PATH = "/tmp/input/"
    OUTPUT_PATH = "/tmp/output/"

    ANOS = [2023]

    TABLE_LIST = {
        "votacao_microdados": "votacoes",
        "votacao_orientacao_bancada": "votacoesOrientacoes",
        "voto_parlamentar": "votacoesVotos",
        "votacao_objeto": "votacoesObjetos",
        "votacao_proposicao_afetada": "votacoesProposicoes",
    }

    RENAME_COLUMNS_OBJETO = {
        "ano": "ano",
        "idVotacao": "id_votacao",
        "uriVotacao": "uriVotacao",
        "data": "data",
        "descricao": "descricao",
        "proposicao_id": "id_proposicao",
        "proposicao_uri": "proposicao_uri",
        "proposicao_ementa": "ementa",
        "proposicao_codTipo": "codigo_tipo",
        "proposicao_siglaTipo": "sigla_tipo",
        "proposicao_numero": "numero",
        "proposicao_ano": "ano_proposicao",
        "proposicao_titulo": "titulo",
    }

    # ------------------------------------------------------------> DEPUTADOS

    TABLE_LIST_DEPUTADOS = {
        "deputado_microdados": "deputados",
        "deputado_ocupacao": "deputadosOcupacoes",
        "deputado_profissao": "deputadosProfissoes",
    }

    TABLE_NAME_ARCHITECTURE_DEPUTADOS = {
        "deputados": "https://docs.google.com/spreadsheets/d/1qfcR5CyUxwa4423mtA8q_XAbgDOEFcPHtKmsg7ZpgyU/edit#gid=0",
        "deputado_ocupado": "https://docs.google.com/spreadsheets/d/1Cj6WE3jk63p21IjrINeaYKoMSOGoDDf1XpY3UH8sct4/edit#gid=0",
        "deputado_profissao": "https://docs.google.com/spreadsheets/d/12R2OY7eqUKxuojcpYYBsCiHyzUOLBBdObnkuv2JUMNI/edit#gid=0",
    }
