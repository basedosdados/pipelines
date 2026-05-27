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
            "segmentada": False,  # Defines whether the table is split into files
            "dicionario": True,  # Defines whether the table comprises the dictionary
            "manual": False,  # Defines whether the dictionary table should be created manually
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
            "manual": False,
            "relationships": [
                {
                    "id_tabela": "estabelecimentos",
                    "nome_coluna": "motivo_situacao_cadastral",
                }
            ],
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
            "manual": False,
            "relationships": [
                {"id_tabela": "empresas", "nome_coluna": "natureza_juridica"}
            ],
        },
        "paises": {
            "table_name": "Paises",
            "segmentada": False,
            "dicionario": True,
            "manual": False,
            "relationships": [
                {"id_tabela": "socios", "nome_coluna": "id_pais"},
                {"id_tabela": "estabelecimentos", "nome_coluna": "id_pais"},
            ],
        },
        "qualificacoes": {
            "table_name": "Qualificacoes",
            "segmentada": False,
            "dicionario": True,
            "manual": False,
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
        "identificador_matriz_filial": {
            "table_name": "Identificador Matriz Filial",
            "segmentada": False,
            "dicionario": True,
            "manual": True,
            "chaves_valores": [
                {"chave": "2", "valor": "Filial"},
                {"chave": "1", "valor": "Matriz"},
            ],
            "relationships": [
                {
                    "id_tabela": "estabelecimentos",
                    "nome_coluna": "identificador_matriz_filial",
                },
            ],
        },
        "situacao_cadastral": {
            "table_name": "Situacao Cadastral",
            "segmentada": False,
            "dicionario": True,
            "manual": True,
            "chaves_valores": [
                {"chave": "2", "valor": "Ativa"},
                {"chave": "8", "valor": "Baixada"},
                {"chave": "4", "valor": "Inapta"},
                {"chave": "1", "valor": "Nula"},
                {"chave": "3", "valor": "Suspensa"},
            ],
            "relationships": [
                {
                    "id_tabela": "estabelecimentos",
                    "nome_coluna": "situacao_cadastral",
                },
            ],
        },
        "faixa_etaria": {
            "table_name": "Faixa Etaria",
            "segmentada": False,
            "dicionario": True,
            "manual": True,
            "chaves_valores": [
                {"chave": "1", "valor": "Entre 0 E 12 Anos"},
                {"chave": "2", "valor": "Entre 13 E 20 Anos"},
                {"chave": "3", "valor": "Entre 21 E 30 Anos"},
                {"chave": "4", "valor": "Entre 31 E 40 Anos"},
                {"chave": "5", "valor": "Entre 41 E 50 Anos"},
                {"chave": "6", "valor": "Entre 51 E 60 Anos"},
                {"chave": "7", "valor": "Entre 61 E 70 Anos"},
                {"chave": "8", "valor": "Entre 71 E 80 Anos"},
                {"chave": "9", "valor": "Mais De 80 Anos"},
                {"chave": "0", "valor": "Não Se Aplica"},
            ],
            "relationships": [
                {"id_tabela": "socios", "nome_coluna": "faixa_etaria"},
            ],
        },
        "porte": {
            "table_name": "Porte",
            "segmentada": False,
            "dicionario": True,
            "manual": True,
            "chaves_valores": [
                {"chave": "5", "valor": "Demais"},
                {"chave": "3", "valor": "Empresa De Pequeno Porte"},
                {"chave": "1", "valor": "Micro Empresa"},
                {"chave": "0", "valor": "Não Informado"},
            ],
            "relationships": [
                {"id_tabela": "empresas", "nome_coluna": "porte"}
            ],
        },
        "tipo": {
            "table_name": "Tipo",
            "segmentada": False,
            "dicionario": True,
            "manual": True,
            "chaves_valores": [
                {"chave": "3", "valor": "Estrangeiro"},
                {"chave": "2", "valor": "Pessoa Física"},
                {"chave": "1", "valor": "Pessoa Jurídica"},
            ],
            "relationships": [{"id_tabela": "socios", "nome_coluna": "tipo"}],
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
