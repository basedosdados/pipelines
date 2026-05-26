"""
Constant values for the datasets projects
"""

###############################################################################

from enum import Enum
from pathlib import Path


class constants(Enum):
    """
    Constant values for the r_me_cnpj project
    """

    ROOT_PATH = Path(__file__).parent.parent.parent.parent
    TMP_PATH = ROOT_PATH / "tmp"
    DATASET_PATH = TMP_PATH / "br_me_cnpj"
    INPUT_PATH = DATASET_PATH / "input"
    OUTPUT_PATH = DATASET_PATH / "output"

    TMP_PATH.mkdir(exist_ok=True, parents=True)
    DATASET_PATH.mkdir(exist_ok=True, parents=True)
    INPUT_PATH.mkdir(exist_ok=True, parents=True)
    OUTPUT_PATH.mkdir(exist_ok=True, parents=True)

    MAX_ATTEMPTS = 3
    TIMEOUT = 5
    ATTEMPTS = 0

    TABLE_CONFIGS = {
        "cnaes": {
            "table_name": "Cnaes",
            "segmentada": False,  # Define se a tabela está dividida em arquivos
            "dicionario": True,  # Define se a tabela compõe o dicionário
            "relationships": [
                {
                    "id_tabela": "estabelecimentos",
                    "nome_coluna": "cnae_fiscal_principal",
                },
                {
                    "id_tabela": "estabelecimentos",
                    "nome_coluna": "cnae_fiscal_secundaria",
                },
            ],
            "n_caracteres": 7,
        },
        "empresas": {
            "table_name": "Empresas",
            "segmentada": True,
            "dicionario": False,
        },
        "estabelecimentos": {
            "table_name": "Estabelecimentos",
            "segmentada": True,
            "dicionario": False,
        },
        "motivos": {
            "table_name": "Motivos",
            "segmentada": False,
            "dicionario": True,
            "relationships": [
                {
                    "id_tabela": "estabelecimentos",
                    "nome_coluna": "motivo_situacao_cadastral",
                }
            ],
            "n_caracteres": 2,
        },
        "municipios": {
            "table_name": "Municipios",
            "segmentada": False,
            "dicionario": False,
        },
        "naturezas": {
            "table_name": "Naturezas",
            "segmentada": False,
            "dicionario": True,
            "relationships": [
                {"id_tabela": "empresas", "nome_coluna": "natureza_juridica"}
            ],
            "n_caracteres": 4,
        },
        "paises": {
            "table_name": "Paises",
            "segmentada": False,
            "dicionario": True,
            "relationships": [
                {"id_tabela": "socios", "nome_coluna": "id_pais"},
                {"id_tabela": "estabelecimentos", "nome_coluna": "id_pais"},
            ],
            "n_caracteres": 3,
        },
        "qualificacoes": {
            "table_name": "Qualificacoes",
            "segmentada": False,
            "dicionario": True,
            "relationships": [
                {
                    "id_tabela": "empresas",
                    "nome_coluna": "qualificacao_responsavel",
                },
                {"id_tabela": "socios", "nome_coluna": "qualificacao"},
                {
                    "id_tabela": "socios",
                    "nome_coluna": "qualificacao_representante_legal",
                },
            ],
            "n_caracteres": 2,
        },
        "simples": {
            "table_name": "Simples",
            "segmentada": False,
            "dicionario": False,
        },
        "socios": {
            "table_name": "Socios",
            "segmentada": True,
            "dicionario": False,
        },
    }

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

    URL = "https://arquivos.receitafederal.gov.br/public.php/dav/files/gn672Ad4CF8N6TK/Dados/Cadastros/CNPJ/"

    HEADERS = {
        "Depth": "1",
        "Content-Type": "application/xml",
        "Accept": "application/xml",
        "User-Agent": "Mozilla/5.0",
    }

    XML_BODY = """<?xml version="1.0" encoding="utf-8" ?>
<d:propfind xmlns:d="DAV:">
  <d:allprop/>
</d:propfind>
"""

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

    COLUNAS_DICIONARIO = ["chave", "valor"]

    default_chunk_size = 20 * 1024 * 1024  # 20MB
    default_max_retries = 32
    default_max_parallel = 16
    default_timeout = 1 * 60 * 1000  # 1 minute
