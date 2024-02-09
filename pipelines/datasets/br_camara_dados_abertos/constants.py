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

    TABLES_INPUT_PATH = {
        "proposicao_microdados": f"/tmp/input/proposicoes-{ANOS_ATUAL}.csv",
        "proposicao_autor": f"/tmp/input/proposicoesAutores-{ANOS_ATUAL}.csv",
        "proposicao_tema": f"/tmp/input/proposicoesTemas-{ANOS_ATUAL}.csv",
        "orgao": "/tmp/input/orgaos.csv",
        "orgao_deputado": "/tmp/input/orgaosDeputados-57.csv",
        "evento": f"/tmp/input/eventos-{ANOS_ATUAL}.csv",
        "evento_orgao": f"/tmp/input/eventosOrgaos-{ANOS_ATUAL}.csv",
        "evento_presenca_deputado": f"/tmp/input/eventosPresencaDeputados-{ANOS_ATUAL}.csv",
        "evento_requerimento": f"/tmp/input/eventosRequerimentos-{ANOS_ATUAL}.csv",
        "frente": "/tmp/input/frentes.csv",
        "frente_deputado": "/tmp/input/frentesDeputados.csv",
        "funcionario": "/tmp/input/funcionarios.csv",
    }

    TABLES_INPUT_PATH_LAST_YEAR = {
        "proposicao_microdados": f"/tmp/input/proposicoes-{ANOS}.csv",
        "proposicao_autor": f"/tmp/input/proposicoesAutores-{ANOS}.csv",
        "proposicao_tema": f"/tmp/input/proposicoesTemas-{ANOS}.csv",
        "orgao": "/tmp/input/orgaos.csv",
        "orgao_deputado": "/tmp/input/orgaosDeputados-57.csv",
        "evento": f"/tmp/input/eventos-{ANOS}.csv",
        "evento_orgao": f"/tmp/input/eventosOrgaos-{ANOS}.csv",
        "evento_presenca_deputado": f"/tmp/input/eventosPresencaDeputados-{ANOS}.csv",
        "evento_requerimento": f"/tmp/input/eventosRequerimentos-{ANOS}.csv",
        "frente": "/tmp/input/frentes.csv",
        "frente_deputado": "/tmp/input/frentesDeputados.csv",
        "funcionario": "/tmp/input/funcionarios.csv",
    }

    TABLES_OUTPUT_PATH = {
        "proposicao_microdados": f"/tmp/output/proposicao_microdados/proposicoes_{ANOS_ATUAL}.csv",
        "proposicao_autor": f"/tmp/output/proposicao_autor/proposicoesAutores_{ANOS_ATUAL}.csv",
        "proposicao_tema": f"/tmp/output/proposicao_tema/proposicoesTemas_{ANOS_ATUAL}.csv",
        "orgao": "/tmp/output/orgao/orgaos.csv",
        "orgao_deputado": "/tmp/output/orgao_deputado/orgaosDeputados-57.csv",
        "evento": f"/tmp/output/evento/eventos_{ANOS_ATUAL}.csv",
        "evento_orgao": f"/tmp/output/evento_orgao/eventosOrgaos_{ANOS_ATUAL}.csv",
        "evento_presenca_deputado": f"/tmp/output/evento_presenca_deputado/eventosPresencaDeputados_{ANOS_ATUAL}.csv",
        "evento_requerimento": f"/tmp/output/evento_requerimento/eventosRequerimentos_{ANOS_ATUAL}.csv",
        "frente": "/tmp/output/frente/frentes.csv",
        "frente_deputado": "/tmp/output/frente_deputado/frentesDeputados.csv",
        "funcionario": "/tmp/output/funcionario/funcionarios.csv",
    }

    TABLES_OUTPUT_PATH_LAST_YEAR = {
        "proposicao_microdados": f"/tmp/output/proposicao_microdados/proposicoes_{ANOS}.csv",
        "proposicao_autor": f"/tmp/output/proposicao_autor/proposicoesAutores_{ANOS}.csv",
        "proposicao_tema": f"/tmp/output/proposicao_tema/proposicoesTemas_{ANOS}.csv",
        "orgao": "/tmp/output/orgao/orgaos.csv",
        "orgao_deputado": "/tmp/output/orgao_deputado/orgaosDeputados-57.csv",
        "evento": f"/tmp/output/evento/eventos_{ANOS}.csv",
        "evento_orgao": f"/tmp/output/evento_orgao/eventosOrgaos_{ANOS}.csv",
        "evento_presenca_deputado": f"/tmp/output/evento_presenca_deputado/eventosPresencaDeputados_{ANOS}.csv",
        "evento_requerimento": f"/tmp/output/evento_requerimento/eventosRequerimentos_{ANOS}.csv",
        "frente": "/tmp/output/frente/frentes.csv",
        "frente_deputado": "/tmp/output/frente_deputado/frentesDeputados.csv",
        "funcionario": "/tmp/output/funcionario/funcionarios.csv",
    }

    TABLES_URL = {
        "proposicao_microdados": f"http://dadosabertos.camara.leg.br/arquivos/proposicoes/csv/proposicoes-{ANOS_ATUAL}.csv",
        "proposicao_autor": f"http://dadosabertos.camara.leg.br/arquivos/proposicoesAutores/csv/proposicoesAutores-{ANOS_ATUAL}.csv",
        "proposicao_tema": f"http://dadosabertos.camara.leg.br/arquivos/proposicoesTemas/csv/proposicoesTemas-{ANOS_ATUAL}.csv",
        "orgao": "http://dadosabertos.camara.leg.br/arquivos/orgaos/csv/orgaos.csv",
        "orgao_deputado": "https://dadosabertos.camara.leg.br/arquivos/orgaosDeputados/csv/orgaosDeputados-L57.csv",
        "evento": f"http://dadosabertos.camara.leg.br/arquivos/eventos/csv/eventos-{ANOS_ATUAL}.csv",
        "evento_orgao": f"http://dadosabertos.camara.leg.br/arquivos/eventosOrgaos/csv/eventosOrgaos-{ANOS_ATUAL}.csv",
        "evento_presenca_deputado": f"http://dadosabertos.camara.leg.br/arquivos/eventosPresencaDeputados/csv/eventosPresencaDeputados-{ANOS_ATUAL}.csv",
        "evento_requerimento": f"http://dadosabertos.camara.leg.br/arquivos/eventosRequerimentos/csv/eventosRequerimentos-{ANOS_ATUAL}.csv",
        "frente": "http://dadosabertos.camara.leg.br/arquivos/frentes/csv/frentes.csv",
        "frente_deputado": "http://dadosabertos.camara.leg.br/arquivos/frentesDeputados/csv/frentesDeputados.csv",
        "funcionario": "http://dadosabertos.camara.leg.br/arquivos/funcionarios/csv/funcionarios.csv",
    }

    TABLES_URL_LAST_BY_YEAR = {
        "proposicao_microdados": f"http://dadosabertos.camara.leg.br/arquivos/proposicoes/csv/proposicoes-{ANOS}.csv",
        "proposicao_autor": f"http://dadosabertos.camara.leg.br/arquivos/proposicoesAutores/csv/proposicoesAutores-{ANOS}.csv",
        "proposicao_tema": f"http://dadosabertos.camara.leg.br/arquivos/proposicoesTemas/csv/proposicoesTemas-{ANOS}.csv",
        "orgao": "http://dadosabertos.camara.leg.br/arquivos/orgaos/csv/orgaos.csv",
        "orgao_deputado": "https://dadosabertos.camara.leg.br/arquivos/orgaosDeputados/csv/orgaosDeputados-L57.csv",
        "evento": f"http://dadosabertos.camara.leg.br/arquivos/eventos/csv/eventos-{ANOS}.csv",
        "evento_orgao": f"http://dadosabertos.camara.leg.br/arquivos/eventosOrgaos/csv/eventosOrgaos-{ANOS}.csv",
        "evento_presenca_deputado": f"http://dadosabertos.camara.leg.br/arquivos/eventosPresencaDeputados/csv/eventosPresencaDeputados-{ANOS}.csv",
        "evento_requerimento": f"http://dadosabertos.camara.leg.br/arquivos/eventosRequerimentos/csv/eventosRequerimentos-{ANOS}.csv",
        "frente": "http://dadosabertos.camara.leg.br/arquivos/frentes/csv/frentes.csv",
        "frente_deputado": "http://dadosabertos.camara.leg.br/arquivos/frentesDeputados/csv/frentesDeputados.csv",
        "funcionario": "http://dadosabertos.camara.leg.br/arquivos/funcionarios/csv/funcionarios.csv",
    }

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
        {"date": "data"},  # proposicao_microdados
        {"date": "data"},  # proposicao_autor
        {"date": "data"},  # proposicao_tema
        {"date": "data_inicio"},  # orgao
        {"date": "data_inicio"},  # orgao_deputado
        {"date": "data_inicio"},  # evento
        {"date": "data"},  # evento_orgao
        {"date": "data"},  # evento_presenca_deputado
        {"date": "data"},  # evento_requerimento
        {"date": "data_criacao"},  # frente
        {"date": "data"},  # frente_deputado
        {"date": "data_inicio_historico"},  # funcionario
    ]
    COVERAGE_TYPE = [
        "part_bdpro",  # proposicao_microdados
        "all_free",  # proposicao_autor
        "all_free",  # proposicao_tema
        "part_bdpro",  # orgao
        "part_bdpro",  # orgao_deputado
        "part_bdpro",  # evento
        "all_free",  # evento_orgao
        "all_free",  # evento_presenca_deputado
        "all_free",  # evento_requerimento
        "part_bdpro",  # frente
        "all_free",  # frente_deputado
        "part_bdpro",  # funcionario
    ]

    HISTORICAL_DATABASE = [
        True,  # proposicao_microdados
        False,  # proposicao_autor
        False,  # proposicao_tema
        True,  # orgao
        True,  # orgao_deputado
        True,  # evento
        False,  # evento_orgao
        False,  # evento_presenca_deputado
        False,  # evento_requerimento
        True,  # frente
        False,  # frente_deputado
        True,  # funcionario
    ]
