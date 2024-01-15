# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""

###############################################################################


from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_me_cnpj project
    """

    MAX_ATTEMPTS = 3
    TIMEOUT = 5
    ATTEMPTS = 0

    TABELAS = ["Empresas", "Socios", "Estabelecimentos", "Simples"]

    UFS = [
        "AC",
        "AL",
        "AM",
        "AP",
        "BA",
        "CE",
        "DF",
        "ES",
        "EX",
        "GO",
        "MA",
        "MG",
        "MS",
        "MT",
        "PA",
        "PB",
        "PE",
        "PI",
        "PR",
        "RJ",
        "RN",
        "RO",
        "RR",
        "RS",
        "SC",
        "SE",
        "SP",
        "TO",
    ]

    URL = "https://dadosabertos.rfb.gov.br/CNPJ/"

    HEADERS = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36"
    }

    COLUNAS_EMPRESAS = [
        "cnpj_basico",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "porte",
        "ente_federativo",
    ]

    COLUNAS_SOCIOS = [
        "cnpj_basico",
        "tipo",
        "nome",
        "documento",
        "qualificacao",
        "data_entrada_sociedade",
        "id_pais",
        "cpf_representante_legal",
        "nome_representante_legal",
        "qualificacao_representante_legal",
        "faixa_etaria",
    ]

    COLUNAS_SIMPLES = [
        "cnpj_basico",
        "opcao_simples",
        "data_opcao_simples",
        "data_exclusao_simples",
        "opcao_mei",
        "data_opcao_mei",
        "data_exclusao_mei",
    ]

    COLUNAS_ESTABELECIMENTO = [
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "identificador_matriz_filial",
        "nome_fantasia",
        "situacao_cadastral",
        "data_situacao_cadastral",
        "motivo_situacao_cadastral",
        "nome_cidade_exterior",
        "id_pais",
        "data_inicio_atividade",
        "cnae_fiscal_principal",
        "cnae_fiscal_secundaria",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "sigla_uf",
        "id_municipio_rf",
        "ddd_1",
        "telefone_1",
        "ddd_2",
        "telefone_2",
        "ddd_fax",
        "fax",
        "email",
        "situacao_especial",
        "data_situacao_especial",
    ]

    COLUNAS_ESTABELECIMENTO_ORDEM = [
        "cnpj",
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "identificador_matriz_filial",
        "nome_fantasia",
        "situacao_cadastral",
        "data_situacao_cadastral",
        "motivo_situacao_cadastral",
        "nome_cidade_exterior",
        "id_pais",
        "data_inicio_atividade",
        "cnae_fiscal_principal",
        "cnae_fiscal_secundaria",
        "sigla_uf",
        "id_municipio",
        "id_municipio_rf",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "ddd_1",
        "telefone_1",
        "ddd_2",
        "telefone_2",
        "ddd_fax",
        "fax",
        "email",
        "situacao_especial",
        "data_situacao_especial",
    ]

    default_chunk_size = 2**20  # 1MB

    default_max_retries = 32

    default_max_parallel = 16

    default_timeout = 3 * 60 * 1000  # 3 minutes
