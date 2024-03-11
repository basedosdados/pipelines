# -*- coding: utf-8 -*-
# register constants
from datetime import datetime
from enum import Enum
from prefect import task
from dateutil.relativedelta import relativedelta


class constants(Enum):

    INPUT_PATH = "/tmp/input/"
    OUTPUT_PATH = "/tmp/output/"

    ANO_ATUAL = (datetime.now()).year
    ANO_ANTERIOR = (ANO_ATUAL - 1)

    TABLES_INPUT_PATH = {
        # ! - > Proposição
        "proposicao_microdados": f"/tmp/input/proposicoes-{ANO_ATUAL}.csv",
        "proposicao_autor": f"/tmp/input/proposicoesAutores-{ANO_ATUAL}.csv",
        "proposicao_tema": f"/tmp/input/proposicoesTemas-{ANO_ATUAL}.csv",
        # ! - > Órgão
        "orgao": "/tmp/input/orgaos.csv",
        "orgao_deputado": "/tmp/input/orgaosDeputados-57.csv",
        # ! - > Evento
        "evento": f"/tmp/input/eventos-{ANO_ATUAL}.csv",
        "evento_orgao": f"/tmp/input/eventosOrgaos-{ANO_ATUAL}.csv",
        "evento_presenca_deputado": f"/tmp/input/eventosPresencaDeputados-{ANO_ATUAL}.csv",
        "evento_requerimento": f"/tmp/input/eventosRequerimentos-{ANO_ATUAL}.csv",
        # ! - > Frente
        "frente": "/tmp/input/frentes.csv",
        "frente_deputado": "/tmp/input/frentesDeputados.csv",
        # ! - > Funcionario
        "funcionario": "/tmp/input/funcionarios.csv",
        # ! - > Votação
        "votacao" : f"/tmp/input/votacoes-{ANO_ATUAL}.csv",
        "votacao_orientacao_bancada": f"/tmp/input/votacoesOrientacoes-{ANO_ATUAL}.csv",
        "voto_parlamentar": f"/tmp/input/votacoesVotos-{ANO_ATUAL}.csv",
        "votacao_objeto": f"/tmp/input/votacoesObjetos-{ANO_ATUAL}.csv",
        "votacao_proposicao": f"/tmp/input/votacoesProposicoes-{ANO_ATUAL}.csv",
        # ! - > Deputado
        "deputado": "/tmp/input/deputados.csv",
        "deputado_ocupacao": "/tmp/input/deputadosOcupacoes.csv",
        "deputado_profissao": "/tmp/input/deputadosProfissoes.csv",
        # ! - > Licitação
        "licitacao" : f"/tmp/input/licitacoes-{ANO_ATUAL}.csv",
        "licitacao_contrato" : f"/tmp/input/licitacoesContratos-{ANO_ATUAL}.csv",
        "licitacao_item" : f"/tmp/input/licitacoesItens-{ANO_ATUAL}.csv",
        "licitacao_pedido" : f"/tmp/input/licitacoesPedidos-{ANO_ATUAL}.csv",
        "licitacao_proposta" : f"/tmp/input/licitacoesPropostas-{ANO_ATUAL}.csv",
        # ! - > Despesa
        "despesa" : f"/tmp/input/Ano-{ANO_ATUAL}.csv"
    }

    TABLES_OUTPUT_PATH = {
        # ! - > Proposição
        "proposicao_microdados": f"/tmp/output/proposicao_microdados/proposicoes_{ANO_ATUAL}.csv",
        "proposicao_autor": f"/tmp/output/proposicao_autor/proposicoesAutores_{ANO_ATUAL}.csv",
        "proposicao_tema": f"/tmp/output/proposicao_tema/proposicoesTemas_{ANO_ATUAL}.csv",
        # ! - > Órgão
        "orgao": "/tmp/output/orgao/orgaos.csv",
        "orgao_deputado": "/tmp/output/orgao_deputado/orgaosDeputados-57.csv",
        # ! - > Evento
        "evento": f"/tmp/output/evento/eventos_{ANO_ATUAL}.csv",
        "evento_orgao": f"/tmp/output/evento_orgao/eventosOrgaos_{ANO_ATUAL}.csv",
        "evento_presenca_deputado": f"/tmp/output/evento_presenca_deputado/eventosPresencaDeputados_{ANO_ATUAL}.csv",
        "evento_requerimento": f"/tmp/output/evento_requerimento/eventosRequerimentos_{ANO_ATUAL}.csv",
        # ! - > Frente
        "frente": "/tmp/output/frente/frentes.csv",
        "frente_deputado": "/tmp/output/frente_deputado/frentesDeputados.csv",
        # ! - > Funcionario
        "funcionario": "/tmp/output/funcionario/funcionarios.csv",
        # ! - > Votação
        "votacao" : f"/tmp/output/votacao/votacoes_{ANO_ATUAL}.csv",
        "votacao_orientacao_bancada": f"/tmp/output/votacao_orientacao_bancada/votacoesOrientacoes_{ANO_ATUAL}.csv",
        "voto_parlamentar": f"/tmp/output/voto_parlamentar/votacoesVotos_{ANO_ATUAL}.csv",
        "votacao_objeto": f"/tmp/output/votacao_objeto/votacoesObjetos_{ANO_ATUAL}.csv",
        "votacao_proposicao": f"/tmp/output/proposicao/votacoesProposicoes_{ANO_ATUAL}.csv",
        # ! - > Deputado
        "deputado": "/tmp/output/deputado/deputados.csv",
        "deputado_ocupacao": "/tmp/output/deputadosOcupacoes.csv",
        "deputado_profissao": "/tmp/output/deputado_profissao/deputadosProfissoes.csv",
        # ! - > Licitação
        "licitacao" : f"/tmp/output/licitacao/licitacoes_{ANO_ATUAL}.csv",
        "licitacao_contrato" : f"/tmp/output/licitacao_contrato/licitacoesContratos_{ANO_ATUAL}.csv",
        "licitacao_item" : f"/tmp/output/licitacao_item/licitacoesItens_{ANO_ATUAL}.csv",
        "licitacao_pedido" : f"/tmp/output/licitacao_pedido/licitacoesPedidos_{ANO_ATUAL}.csv",
        "licitacao_proposta" : f"/tmp/output/licitacao_proposta/licitacoesPropostas_{ANO_ATUAL}.csv",
        # ! - > Despesa
        "despesa" : f"/tmp/output/despesa/despesa_{ANO_ATUAL}.csv"
    }

    TABLES_URL = {
        # ! - > Proposição
        "proposicao_microdados": f"http://dadosabertos.camara.leg.br/arquivos/proposicoes/csv/proposicoes-{ANO_ATUAL}.csv",
        "proposicao_autor": f"http://dadosabertos.camara.leg.br/arquivos/proposicoesAutores/csv/proposicoesAutores-{ANO_ATUAL}.csv",
        "proposicao_tema": f"http://dadosabertos.camara.leg.br/arquivos/proposicoesTemas/csv/proposicoesTemas-{ANO_ATUAL}.csv",
        # ! - > Órgão
        "orgao": "http://dadosabertos.camara.leg.br/arquivos/orgaos/csv/orgaos.csv",
        "orgao_deputado": "https://dadosabertos.camara.leg.br/arquivos/orgaosDeputados/csv/orgaosDeputados-L57.csv",
        # ! - > Evento
        "evento": f"http://dadosabertos.camara.leg.br/arquivos/eventos/csv/eventos-{ANO_ATUAL}.csv",
        "evento_orgao": f"http://dadosabertos.camara.leg.br/arquivos/eventosOrgaos/csv/eventosOrgaos-{ANO_ATUAL}.csv",
        "evento_presenca_deputado": f"http://dadosabertos.camara.leg.br/arquivos/eventosPresencaDeputados/csv/eventosPresencaDeputados-{ANO_ATUAL}.csv",
        "evento_requerimento": f"http://dadosabertos.camara.leg.br/arquivos/eventosRequerimentos/csv/eventosRequerimentos-{ANO_ATUAL}.csv",
        # ! - > Frente
        "frente": "http://dadosabertos.camara.leg.br/arquivos/frentes/csv/frentes.csv",
        "frente_deputado": "http://dadosabertos.camara.leg.br/arquivos/frentesDeputados/csv/frentesDeputados.csv",
        # ! - > Funcionario
        "funcionario": "http://dadosabertos.camara.leg.br/arquivos/funcionarios/csv/funcionarios.csv",
        # ! - > Votação
        "votacao" : f"https://dadosabertos.camara.leg.br/arquivos/votacoes/csv/votacoes-{ANO_ATUAL}.csv",
        "votacao_orientacao_bancada": f"https://dadosabertos.camara.leg.br/arquivos/votacoesOrientacoes/csv/votacoesOrientacoes-{ANO_ATUAL}.csv",
        "voto_parlamentar": f"https://dadosabertos.camara.leg.br/arquivos/votacoesVotos/csv/votacoesVotos-{ANO_ATUAL}.csv",
        "votacao_objeto": f"https://dadosabertos.camara.leg.br/arquivos/votacoesObjetos/csv/votacoesObjetos-{ANO_ATUAL}.csv",
        "votacao_proposicao": f"https://dadosabertos.camara.leg.br/arquivos/votacoesProposicoes/csv/votacoesProposicoes-{ANO_ATUAL}.csv",
        # ! - > Deputado
        "deputado": "https://dadosabertos.camara.leg.br/arquivos/deputados/csv/deputados.csv",
        "deputado_ocupacao": "https://dadosabertos.camara.leg.br/arquivos/deputadosOcupacoes/csv/deputadosOcupacoes.csv",
        "deputado_profissao": "https://dadosabertos.camara.leg.br/arquivos/deputadosProfissoes/csv/deputadosProfissoes.csv",
        # ! - > Licitação
        "licitacao" : f"https://dadosabertos.camara.leg.br/arquivos/licitacoes/csv/licitacoes-{ANO_ATUAL}.csv",
        "licitacao_contrato" : f"https://dadosabertos.camara.leg.br/arquivos/licitacoesContratos/csv/licitacoesContratos-{ANO_ATUAL}.csv",
        "licitacao_item" : f"https://dadosabertos.camara.leg.br/arquivos/licitacoesItens/csv/licitacoesItens-{ANO_ATUAL}.csv",
        "licitacao_pedido" : f"https://dadosabertos.camara.leg.br/arquivos/licitacoesPedidos/csv/licitacoesPedidos-{ANO_ATUAL}.csv",
        "licitacao_proposta" : f"https://dadosabertos.camara.leg.br/arquivos/licitacoesPropostas/csv/licitacoesPropostas-{ANO_ATUAL}.csv",
        # ! - > Despesa
        "despesa" : f"https://www.camara.leg.br/cotas/Ano-{ANO_ATUAL}.csv.zip"
    }

    TABLES_URL_ANO_ANTERIOR = {
        # ! - > Proposição
        "proposicao_microdados": f"http://dadosabertos.camara.leg.br/arquivos/proposicoes/csv/proposicoes-{ANO_ANTERIOR}.csv",
        "proposicao_autor": f"http://dadosabertos.camara.leg.br/arquivos/proposicoesAutores/csv/proposicoesAutores-{ANO_ANTERIOR}.csv",
        "proposicao_tema": f"http://dadosabertos.camara.leg.br/arquivos/proposicoesTemas/csv/proposicoesTemas-{ANO_ANTERIOR}.csv",
        # ! - > Órgão
        "orgao": "http://dadosabertos.camara.leg.br/arquivos/orgaos/csv/orgaos.csv",
        "orgao_deputado": "https://dadosabertos.camara.leg.br/arquivos/orgaosDeputados/csv/orgaosDeputados-L57.csv",
        # ! - > Evento
        "evento": f"http://dadosabertos.camara.leg.br/arquivos/eventos/csv/eventos-{ANO_ANTERIOR}.csv",
        "evento_orgao": f"http://dadosabertos.camara.leg.br/arquivos/eventosOrgaos/csv/eventosOrgaos-{ANO_ANTERIOR}.csv",
        "evento_presenca_deputado": f"http://dadosabertos.camara.leg.br/arquivos/eventosPresencaDeputados/csv/eventosPresencaDeputados-{ANO_ANTERIOR}.csv",
        "evento_requerimento": f"http://dadosabertos.camara.leg.br/arquivos/eventosRequerimentos/csv/eventosRequerimentos-{ANO_ANTERIOR}.csv",
        # ! - > Frente
        "frente": "http://dadosabertos.camara.leg.br/arquivos/frentes/csv/frentes.csv",
        "frente_deputado": "http://dadosabertos.camara.leg.br/arquivos/frentesDeputados/csv/frentesDeputados.csv",
        # ! - > Funcionario
        "funcionario": "http://dadosabertos.camara.leg.br/arquivos/funcionarios/csv/funcionarios.csv",
        # ! - > Votação
        "votacao" : f"https://dadosabertos.camara.leg.br/arquivos/votacoes/csv/votacoes-{ANO_ANTERIOR}.csv",
        "votacao_orientacao_bancada": f"https://dadosabertos.camara.leg.br/arquivos/votacoesOrientacoes/csv/votacoesOrientacoes-{ANO_ANTERIOR}.csv",
        "voto_parlamentar": f"https://dadosabertos.camara.leg.br/arquivos/votacoesVotos/csv/votacoesVotos-{ANO_ANTERIOR}.csv",
        "votacao_objeto": f"https://dadosabertos.camara.leg.br/arquivos/votacoesObjetos/csv/votacoesObjetos-{ANO_ANTERIOR}.csv",
        "votacao_proposicao": f"https://dadosabertos.camara.leg.br/arquivos/votacoesProposicoes/csv/votacoesProposicoes-{ANO_ANTERIOR}.csv",
        # ! - > Deputado
        "deputado": "https://dadosabertos.camara.leg.br/arquivos/deputados/csv/deputados.csv",
        "deputado_ocupacao": "https://dadosabertos.camara.leg.br/arquivos/deputadosOcupacoes/csv/deputadosOcupacoes.csv",
        "deputado_profissao": "https://dadosabertos.camara.leg.br/arquivos/deputadosProfissoes/csv/deputadosProfissoes.csv",
        # ! - > Licitação
        "licitacao" : f"https://dadosabertos.camara.leg.br/arquivos/licitacoes/csv/licitacoes-{ANO_ANTERIOR}.csv",
        "licitacao_contrato" : f"https://dadosabertos.camara.leg.br/arquivos/licitacoesContratos/csv/licitacoesContratos-{ANO_ANTERIOR}.csv",
        "licitacao_item" : f"https://dadosabertos.camara.leg.br/arquivos/licitacoesItens/csv/licitacoesItens-{ANO_ANTERIOR}.csv",
        "licitacao_pedido" : f"https://dadosabertos.camara.leg.br/arquivos/licitacoesPedidos/csv/licitacoesPedidos-{ANO_ANTERIOR}.csv",
        "licitacao_proposta" : f"https://dadosabertos.camara.leg.br/arquivos/licitacoesPropostas/csv/licitacoesPropostas-{ANO_ANTERIOR}.csv",
        # ! - > Despesa
        "despesa" : f"https://www.camara.leg.br/cotas/Ano-{ANO_ANTERIOR}.csv.zip"

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

@task
def update_metadata_variable_dictionary(table_id: str, dataset_id = "br_camara_dados_abertos"):
    dict_of_table = {
                                "deputado": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name":{'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "all_free",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : False,
                                },
                                "deputado_ocupacao": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name":{'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "all_free",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : False,
                                },
                                "deputado_profissao": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name":{'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "all_free",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : False,
                                },
                                "evento": {
                                    "dataset_id":dataset_id,
                                    "table_id":table_id,
                                    "date_column_name":{'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "coverage_type":"part_bdpro",
                                    "time_delta":{'months': 6},
                                    "prefect_mode":"prod",
                                    "bq_project":"basedosdados",
                                    "historical_database":True
                                },
                                "evento_orgao": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name":{'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "all_free",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : False,
                                },
                                "evento_presenca_deputado": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "all_free",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "evento_requerimento": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name":{'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "all_free",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : False,
                                },
                                "frente": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data_criacao'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "frente_deputado": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name":{'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "all_free",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : False,
                                },
                                "funcionario": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data_inicio_historico'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "licitacao": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data_abertura'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "licitacao_item": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name":{'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "all_free",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : False,
                                },
                                "licitacao_contrato": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data_assinatura'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "licitacao_pedido": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data_cadastro'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "licitacao_proposta": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name":{'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "all_free",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : False,
                                },
                                "orgao": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "orgao_deputado": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "proposicao_autor": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name":{'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "all_free",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : False,
                                },
                                "proposicao_microdados": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "proposicao_tema": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name":{'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "all_free",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : False,
                                },
                                "votacao": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "votacao_objeto": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "votacao_parlamentar ": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "votacao_proposicao": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name": {'date': 'data'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                },
                                "votacao_orientacao_bancada": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name":{'date': 'data_inicio'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "all_free",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : False,
                                },
                                "despesa": {
                                    "dataset_id": dataset_id,
                                    "table_id": table_id,
                                    "date_column_name":{'date': 'data_emissao'},
                                    "date_format":"%Y-%m-%d",
                                    "time_delta":{'months': 6},
                                    "coverage_type": "part_bdpro",
                                    "prefect_mode": "prod",
                                    "bq_project": "basedosdados",
                                    "historical_database" : True,
                                }
                                }
    return dict_of_table.get(table_id)
