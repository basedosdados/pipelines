# -*- coding: utf-8 -*-
"""
Constants for br_tse_eleicoes pipeline.
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constants for utils.
    """


    QUERY_COUNT_MODIFIED = """SELECT
  (SELECT count(*) as total FROM `{mode}.br_tse_eleicoes.{table_id}` WHERE ano={year}) AS total,
  (SELECT TIMESTAMP_MILLIS(creation_time) as last_modified_time
   FROM `{mode}.br_tse_eleicoes.__TABLES_SUMMARY__`
   WHERE table_id = '{table_id}') AS last_modified_time;"""



    MODE_TO_PROJECT_DICT = {
        "prod": "basedosdados",
        "dev": "basedosdados-dev"
    }


    # Candidtos

    CANDIDATOS24_ZIP = "https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_2024.zip"
    COMPLEMENTR24_ZIP = "https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand_complementar/consulta_cand_complementar_2024.zip"

    MUNICIPIOS_CSV = "https://storage.googleapis.com/basedosdados/staging/br_bd_diretorios_brasil/municipio/municipio.csv"

    QUERY_MUNIPIPIOS = "select id_municipio, id_municipio_tse from `basedosdados.br_bd_diretorios_brasil.municipio`"

    CANDIDATOS_URLS = [CANDIDATOS24_ZIP, COMPLEMENTR24_ZIP]


    ORDER = {'id_eleicao': 'CD_ELEICAO_x',
            'tipo_eleicao': 'NM_TIPO_ELEICAO',
            'data_eleicao': 'DT_ELEICAO',
            'sigla_uf': 'SG_UF',
            'id_municipio': 'id_municipio',
            'id_municipio_tse': 'SG_UE',
            'id_candidato_bd': 'id_candidato_bd',
            'cpf': 'NR_CPF_CANDIDATO',
            'titulo_eleitoral': 'NR_TITULO_ELEITORAL_CANDIDATO',
            'sequencial': 'SQ_CANDIDATO',
            'numero': 'NR_CANDIDATO',
            'nome': 'NM_CANDIDATO',
            'nome_urna': 'NM_URNA_CANDIDATO',
            'numero_partido': 'NR_PARTIDO',
            'sigla_partido': 'SG_PARTIDO',
            'cargo': 'DS_CARGO',
            'email': 'DS_EMAIL',
            'situacao': 'DS_DETALHE_SITUACAO_CAND',
            'nacionalidade': 'DS_NACIONALIDADE',
            'sigla_uf_nascimento': 'SG_UF_NASCIMENTO',
            'municipio_nascimento': 'NM_MUNICIPIO_NASCIMENTO',
            'data_nascimento': 'DT_NASCIMENTO',
            'idade': 'NR_IDADE_DATA_POSSE',
            'genero': 'DS_GENERO',
            'instrucao': 'DS_GRAU_INSTRUCAO',
            'estado_civil': 'DS_ESTADO_CIVIL',
            'raca': 'DS_COR_RACA',
            'ocupacao': 'DS_OCUPACAO'}

    # Constantes BENS CANDIDATO

    BENS_CANDIDATOS24 = "https://cdn.tse.jus.br/estatistica/sead/odsele/bem_candidato/bem_candidato_2024.zip"

    ORDER_BENS = {
            'id_eleicao': 'CD_ELEICAO',
            'tipo_eleicao': 'NM_TIPO_ELEICAO',
            'data_eleicao': 'DT_ELEICAO',
            'sigla_uf': 'SG_UF',
            'sequencial_candidato': 'SQ_CANDIDATO',
            'id_candidato_bd': 'id_candidato_bd',
            'id_tipo_item': 'CD_TIPO_BEM_CANDIDATO',
            'tipo_item': 'DS_TIPO_BEM_CANDIDATO',
            'descricao_item': 'DS_BEM_CANDIDATO',
            'valor_item': 'VR_BEM_CANDIDATO'
            }

    # Despesas Candidato

    DESPESAS_RECEITAS24 = "https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas/prestacao_de_contas_eleitorais_candidatos_2024.zip"

    ORDER_DESPESAS = {"turno": "ST_TURNO", "id_eleicao": "CD_ELEICAO", "tipo_eleicao": "DS_ELEICAO",
    "data_eleicao":"DT_ELEICAO", "sigla_uf": "SG_UF", "id_municipio": "id_municipio", "id_municipio_tse": "SG_UE",
    "sequencial_candidato": "SQ_CANDIDATO", "numero_candidato": "NR_CANDIDATO",
    "cpf_candidato": "NR_CPF_CANDIDATO", "id_candidato_bd": "id_candidato_bd", "nome_candidato": "NM_CANDIDATO",
    "cpf_vice_suplente": "NR_CPF_VICE_CANDIDATO", "numero_partido": "NR_PARTIDO",
    "sigla_partido": "SG_PARTIDO", "nome_partido": "NM_PARTIDO", "cargo": "DS_CARGO",
    "sequencial_despesa": "SQ_DESPESA", "data_despesa": "DT_DESPESA", "tipo_despesa": "tipo_despesa",
    "descricao_despesa": "DS_DESPESA", "origem_despesa": "DS_ORIGEM_DESPESA", "valor_despesa": "VR_DESPESA_CONTRATADA",
    "tipo_prestacao_contas": "TP_PRESTACAO_CONTAS", "data_prestacao_contas": "DT_PRESTACAO_CONTAS",
    "sequencial_prestador_contas": "SQ_PRESTADOR_CONTAS", "cnpj_prestador_contas": "NR_CNPJ_PRESTADOR_CONTA",
    "cnpj_candidato": "cnpj_candidato", "tipo_documento": "DS_TIPO_DOCUMENTO", "numero_documento": "NR_DOCUMENTO",
    "especie_recurso": "especie_recurso", "fonte_recurso": "fonte_recurso", "cpf_cnpj_fornecedor": "NR_CPF_CNPJ_FORNECEDOR",
    "nome_fornecedor": "NM_FORNECEDOR", "nome_fornecedor_rf": "NM_FORNECEDOR_RFB",
    "cnae_2_fornecedor": "CD_CNAE_FORNECEDOR", "descricao_cnae_2_fornecedor": "DS_CNAE_FORNECEDOR", "tipo_fornecedor": "DS_TIPO_FORNECEDOR",
    "esfera_partidaria_fornecedor": "esfera_partidaria_fornecedor", "sigla_uf_fornecedor": "SG_UF_FORNECEDOR", "id_municipio_tse_fornecedor": "CD_MUNICIPIO_FORNECEDOR",
    "sequencial_candidato_fornecedor": "SQ_CANDIDATO_FORNECEDOR", "numero_candidato_fornecedor": "NR_CANDIDATO_FORNECEDOR", "numero_partido_fornecedor": "NR_PARTIDO_FORNECEDOR",
    "sigla_partido_fornecedor": "SG_PARTIDO_FORNECEDOR", "nome_partido_fornecedor": "NM_PARTIDO_FORNECEDOR", "cargo_fornecedor": "DS_CARGO_FORNECEDOR"}
