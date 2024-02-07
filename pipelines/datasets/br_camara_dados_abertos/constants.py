# -*- coding: utf-8 -*-
from datetime import datetime
from enum import Enum

from dateutil.relativedelta import relativedelta


class constants(Enum):
    TABLE_NAME_ARCHITECTURE = {
        "votacao_proposicao_afetada": "https://docs.google.com/spreadsheets/d/1sHBdS7dgGAWlegMs1rEy6OwUVDwTLcZy92pFX6U5rXM/edit#gid=0",
        "votacao_parlamentar": "https://docs.google.com/spreadsheets/d/171Mykmg5qz54Kp35XgSD_IshngQHYE3jfGZ8yL-wEpE/edit#gid=0",
        "votacao_microdados": "https://docs.google.com/spreadsheets/d/1GZjzBqAQ5RqaB6kyOjD7iZeKSPxB07lAkZnvaGjREIY/edit#gid=0",
        "votacao_objeto": "https://docs.google.com/spreadsheets/d/1w2r5eK8jx3SwMTo83LKiI-wK_UE26SlTrJFqe_GZw4E/edit#gid=0",
        "votacao_orientacao_bancada": "https://docs.google.com/spreadsheets/d/1_gl5llaGw5Mr0A6Q8AUoHHiOiEGl7Ht8Yf3p2n4k8uA/edit#gid=0",
    }

    INPUT_PATH = "/tmp/input/"
    OUTPUT_PATH = "/tmp/output/"

    ANOS = (datetime.now() - relativedelta(years=1)).year

    ANOS_ATUAL = (datetime.now()).year

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
        "deputados": "deputados",
        "deputado_ocupacao": "deputadosOcupacoes",
        "deputado_profissao": "deputadosProfissoes",
    }

    TABLE_NAME_ARCHITECTURE_DEPUTADOS = {
        "deputados": "https://docs.google.com/spreadsheets/d/1qfcR5CyUxwa4423mtA8q_XAbgDOEFcPHtKmsg7ZpgyU/edit#gid=0",
        "deputado_ocupacao": "https://docs.google.com/spreadsheets/d/1Cj6WE3jk63p21IjrINeaYKoMSOGoDDf1XpY3UH8sct4/edit#gid=0",
        "deputado_profissao": "https://docs.google.com/spreadsheets/d/12R2OY7eqUKxuojcpYYBsCiHyzUOLBBdObnkuv2JUMNI/edit#gid=0",
    }

    # ------------------------------------------------------------> PROPOSIÇÃO

    TABLE_LIST_CAMARA = {
        "proposicao_microdados": "proposicoes",
        "proposicao_autor": "proposicoesAutores",
        "proposicao_tema": "proposicoesTemas",
        "orgao": "orgaos",
        "orgao_deputado": "orgaosDeputados",
        "evento": "eventos",
        "evento_orgao": "eventosOrgaos",
        "evento_presenca_deputado": "eventosPresencaDeputados",
        "evento_requerimento": "eventosRequerimentos",
        "frente": "frentes",
        "frente_deputado": "frentesDeputados",
        "funcionario": "funcionarios",
    }

    TABLES_SPLIT_BY_YEAR = [
        "proposicao_microdados",
        "proposicao_autor",
        "proposicao_tema",
        "evento",
        "evento_orgao",
        "evento_presenca_deputado",
        "evento_requerimento",
    ]

    TABLES_SPLIT_WITHOUT_YEAR = ["orgao", "frente_deputado", "frente", "funcionario"]

    RENAME_COLUMNS_FRENTE_DEPUTADO = {
        "deputado_.id": "id_deputado",
        "deputado_.nome": "nome_deputado",
        "deputado_.titulo": "titulo_deputado",
        "deputado_.siglaUf": "sigla_uf_deputado",
        "deputado_.uri": "url_deputado",
        "deputado_.uriPartido": "url_partido_deputado",
        "deputado_.idLegislatura": "id_legislatura_deputado",
        "deputado_.urlFoto": "url_foto_deputado",
        "deputado_.codTitulo": "cod_titulo_deputado",
    }

    RENAME_COLUMNS_EVENTO = {
        "localCamara.nome": "localCamara_nome",
        "localCamara.predio": "localCamara_predio",
        "localCamara.sala": "localCamara_sala",
        "localCamara.andar": "localCamara_andar",
    }

    DATA_COLUMN_NAME = [
        {"date": "data"},
        {"date": "data"},
        {"date": "data"},
        {"date": "data_inicio"},
        {"date": "data_inicio"},
    ]
    COVERAGE_TYPE = [
        "part_bdpro",
        "all_free",
        "all_free",
        "part_bdpro",
        "part_bdpro",
    ]

    HISTORICAL_DATABASE = [True, False, False, True, True]
