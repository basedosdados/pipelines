# -*- coding: utf-8 -*-
# register constants
from datetime import datetime
from enum import Enum

from dateutil.relativedelta import relativedelta


class constants(Enum):

    INPUT_PATH = "/tmp/input/"
    OUTPUT_PATH = "/tmp/output/"

    ANO_ANTERIOR = (datetime.now() - relativedelta(years=1)).year

    ANO_ATUAL = (datetime.now()).year

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
        "votacao_microdados": "votacoes",
        "votacao_orientacao_bancada": "votacoesOrientacoes",
        "voto_parlamentar": "votacoesVotos",
        "votacao_objeto": "votacoesObjetos",
        "votacao_proposicao_afetada": "votacoesProposicoes",
        "deputados": "deputados",
        "deputado_ocupacao": "deputadosOcupacoes",
        "deputado_profissao": "deputadosProfissoes",
    }

    TABLES_INPUT_PATH = {
        # - > Proposição
        "proposicao_microdados": f"/tmp/input/proposicoes-{ANO_ATUAL}.csv",
        "proposicao_autor": f"/tmp/input/proposicoesAutores-{ANO_ATUAL}.csv",
        "proposicao_tema": f"/tmp/input/proposicoesTemas-{ANO_ATUAL}.csv",
        # - > Órgão
        "orgao": "/tmp/input/orgaos.csv",
        "orgao_deputado": "/tmp/input/orgaosDeputados-57.csv",
        # - > Evento
        "evento": f"/tmp/input/eventos-{ANO_ATUAL}.csv",
        "evento_orgao": f"/tmp/input/eventosOrgaos-{ANO_ATUAL}.csv",
        "evento_presenca_deputado": f"/tmp/input/eventosPresencaDeputados-{ANO_ATUAL}.csv",
        "evento_requerimento": f"/tmp/input/eventosRequerimentos-{ANO_ATUAL}.csv",
        # - > Frente
        "frente": "/tmp/input/frentes.csv",
        "frente_deputado": "/tmp/input/frentesDeputados.csv",
        # - > Funcionario
        "funcionario": "/tmp/input/funcionarios.csv",
        # - > Votação
        "votacao_microdados" : f"/tmp/input/votacoes-{ANO_ATUAL}.csv",
        "votacao_orientacao_bancada": f"/tmp/input/votacoesOrientacoes-{ANO_ATUAL}.csv",
        "voto_parlamentar": f"/tmp/input/votacoesVotos-{ANO_ATUAL}.csv",
        "votacao_objeto": f"/tmp/input/votacoesObjetos-{ANO_ATUAL}.csv",
        "votacao_proposicao_afetada": f"/tmp/input/votacoesProposicoes-{ANO_ATUAL}.csv",
        # - > Deputado
        "deputados": "/tmp/input/deputados.csv",
        "deputado_ocupacao": "/tmp/input/deputado_ocupacao.csv",
        "deputado_profissao": "/tmp/input/deputado_profissao/deputadosProfissoes.csv",
    }

    TABLES_OUTPUT_PATH = {
        # - > Proposição
        "proposicao_microdados": f"/tmp/output/proposicao_microdados/proposicoes_{ANO_ATUAL}.csv",
        "proposicao_autor": f"/tmp/output/proposicao_autor/proposicoesAutores_{ANO_ATUAL}.csv",
        "proposicao_tema": f"/tmp/output/proposicao_tema/proposicoesTemas_{ANO_ATUAL}.csv",
        # - > Órgão
        "orgao": "/tmp/output/orgao/orgaos.csv",
        "orgao_deputado": "/tmp/output/orgao_deputado/orgaosDeputados-57.csv",
        # - > Evento
        "evento": f"/tmp/output/evento/eventos_{ANO_ATUAL}.csv",
        "evento_orgao": f"/tmp/output/evento_orgao/eventosOrgaos_{ANO_ATUAL}.csv",
        "evento_presenca_deputado": f"/tmp/output/evento_presenca_deputado/eventosPresencaDeputados_{ANO_ATUAL}.csv",
        "evento_requerimento": f"/tmp/output/evento_requerimento/eventosRequerimentos_{ANO_ATUAL}.csv",
        # - > Frente
        "frente": "/tmp/output/frente/frentes.csv",
        "frente_deputado": "/tmp/output/frente_deputado/frentesDeputados.csv",
        # - > Funcionario
        "funcionario": "/tmp/output/funcionario/funcionarios.csv",
        # - > Votação
        "votacao_microdados" : f"/tmp/output/votacao_microdados/votacoes_{ANO_ATUAL}.csv",
        "votacao_orientacao_bancada": f"/tmp/output/votacao_orientacao_bancada/votacoesOrientacoes_{ANO_ATUAL}.csv",
        "voto_parlamentar": f"/tmp/output/voto_parlamentar/votacoesVotos_{ANO_ATUAL}.csv",
        "votacao_objeto": f"/tmp/output/votacao_objeto/votacoesObjetos_{ANO_ATUAL}.csv",
        "votacao_proposicao_afetada": f"/tmp/output/votacao_proposicao_afetada/votacoesProposicoes_{ANO_ATUAL}.csv",
        # - > Deputado
        "deputados": "/tmp/output/deputados/deputados.csv",
        "deputado_ocupacao": "/tmp/output/deputado_ocupacao.csv",
        "deputado_profissao": "/tmp/output/deputado_profissao/deputadosProfissoes.csv",
    }

    TABLES_URL = {
        # - > Proposição
        "proposicao_microdados": f"http://dadosabertos.camara.leg.br/arquivos/proposicoes/csv/proposicoes-{ANO_ATUAL}.csv",
        "proposicao_autor": f"http://dadosabertos.camara.leg.br/arquivos/proposicoesAutores/csv/proposicoesAutores-{ANO_ATUAL}.csv",
        "proposicao_tema": f"http://dadosabertos.camara.leg.br/arquivos/proposicoesTemas/csv/proposicoesTemas-{ANO_ATUAL}.csv",
        # - > Órgão
        "orgao": "http://dadosabertos.camara.leg.br/arquivos/orgaos/csv/orgaos.csv",
        "orgao_deputado": "https://dadosabertos.camara.leg.br/arquivos/orgaosDeputados/csv/orgaosDeputados-L57.csv",
        # - > Evento
        "evento": f"http://dadosabertos.camara.leg.br/arquivos/eventos/csv/eventos-{ANO_ATUAL}.csv",
        "evento_orgao": f"http://dadosabertos.camara.leg.br/arquivos/eventosOrgaos/csv/eventosOrgaos-{ANO_ATUAL}.csv",
        "evento_presenca_deputado": f"http://dadosabertos.camara.leg.br/arquivos/eventosPresencaDeputados/csv/eventosPresencaDeputados-{ANO_ATUAL}.csv",
        "evento_requerimento": f"http://dadosabertos.camara.leg.br/arquivos/eventosRequerimentos/csv/eventosRequerimentos-{ANO_ATUAL}.csv",
        # - > Frente
        "frente": "http://dadosabertos.camara.leg.br/arquivos/frentes/csv/frentes.csv",
        "frente_deputado": "http://dadosabertos.camara.leg.br/arquivos/frentesDeputados/csv/frentesDeputados.csv",
        # - > Funcionario
        "funcionario": "http://dadosabertos.camara.leg.br/arquivos/funcionarios/csv/funcionarios.csv",
        # - > Votação
        "votacao_microdados" : f"https://dadosabertos.camara.leg.br/arquivos/votacoes/csv/votacoes-{ANO_ATUAL}.csv",
        "votacao_orientacao_bancada": f"https://dadosabertos.camara.leg.br/arquivos/votacoesOrientacoes/csv/votacoesOrientacoes-{ANO_ATUAL}.csv",
        "voto_parlamentar": f"https://dadosabertos.camara.leg.br/arquivos/votacoesVotos/csv/votacoesVotos-{ANO_ATUAL}.csv",
        "votacao_objeto": f"https://dadosabertos.camara.leg.br/arquivos/votacoesObjetos/csv/votacoesObjetos-{ANO_ATUAL}.csv",
        "votacao_proposicao_afetada": f"https://dadosabertos.camara.leg.br/arquivos/votacoesProposicoes/csv/votacoesProposicoes-{ANO_ATUAL}.csv",
        # - > Deputado
        "deputados": "https://dadosabertos.camara.leg.br/arquivos/deputados/csv/deputados.csv",
        "deputado_ocupacao": "https://dadosabertos.camara.leg.br/arquivos/deputadosOcupacoes/csv/deputadosOcupacoes.csv",
        "deputado_profissao": "https://dadosabertos.camara.leg.br/arquivos/deputadosProfissoes/csv/deputadosProfissoes.csv",
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
        {"date": "data_inicio"},  # evento_presenca_deputado
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
        "part_bdpro",  # evento_presenca_deputado
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
        True,  # evento_presenca_deputado
        False,  # evento_requerimento
        True,  # frente
        False,  # frente_deputado
        True,  # funcionario
    ]
