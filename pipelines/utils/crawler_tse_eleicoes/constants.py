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
