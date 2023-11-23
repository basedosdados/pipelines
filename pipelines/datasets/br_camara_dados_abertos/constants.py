# -*- coding: utf-8 -*-
from enum import Enum


class constants(Enum):
    dict_arquitetura = {
        "votacao_proposicao_afetada": "https://docs.google.com/spreadsheets/d/1sHBdS7dgGAWlegMs1rEy6OwUVDwTLcZy92pFX6U5rXM/edit#gid=0",
        "voto_parlamentar": "https://docs.google.com/spreadsheets/d/171Mykmg5qz54Kp35XgSD_IshngQHYE3jfGZ8yL-wEpE/edit#gid=0",
        "votacao_microdados": "https://docs.google.com/spreadsheets/d/1GZjzBqAQ5RqaB6kyOjD7iZeKSPxB07lAkZnvaGjREIY/edit#gid=0",
        "votacao_objeto": "https://docs.google.com/spreadsheets/d/1w2r5eK8jx3SwMTo83LKiI-wK_UE26SlTrJFqe_GZw4E/edit#gid=0",
        "votacao_orientacao_bancada": "https://docs.google.com/spreadsheets/d/1_gl5llaGw5Mr0A6Q8AUoHHiOiEGl7Ht8Yf3p2n4k8uA/edit#gid=0",
    }

    INPUT_PATH = "/tmp/input/"
    OUTPUT_PATH = "/tmp/output/"
    OUTPUT_PATH_MICRODADOS = "/tmp/output/microdados/"
    OUTPUT_PATH_PARLAMENTAR = "/tmp/output/parlamentar/"
    OUTPUT_PATH_PROPOSICAO = "/tmp/output/proposicao/"
    OUTPUT_PATH_OBJETO = "/tmp/output/objeto/"
    OUTPUT_PATH_ORIENTACAO = "/tmp/output/orientacao/"
    ANOS = [2023]

    VOTOS = [
        "votacoes",
        "votacoesOrientacoes",
        "votacoesVotos",
        "votacoesObjetos",
        "votacoesProposicoes",
    ]
