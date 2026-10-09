"""
Constant values for br_rf_cnpj
"""

from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    NonHistorical,
    PartBdpro,
)

DATASET_ID = "br_rf_cnpj"
EMPRESAS_TABLE_ID = "empresas"
ESTABELECIMENTOS_TABLE_ID = "estabelecimentos"
SOCIOS_TABLE_ID = "socios"
SIMPLES_TABLE_ID = "simples"
DICIONARIO_TABLE_ID = "dicionario"

MAX_ATTEMPTS = 3
TIMEOUT = 5
ATTEMPTS = 0

# Parâmetros de download
DOWNLOAD_CHUNK_SIZE = 15 * 1024 * 1024
DOWNLOAD_MAX_RETRIES = 5
DOWNLOAD_MAX_PARALLEL = 15
DOWNLOAD_TIMEOUT = 5 * 60
CSV_CHUNK_SIZE = 100_000

## Cada tabela no BigQuery pode ter mais de uma fonte de dados
## na RF, caso do dicionario
TABLE_COMPONENTS = {
    EMPRESAS_TABLE_ID: ["empresas"],
    SOCIOS_TABLE_ID: ["socios"],
    ESTABELECIMENTOS_TABLE_ID: ["estabelecimentos"],
    SIMPLES_TABLE_ID: ["simples"],
    DICIONARIO_TABLE_ID: [
        "qualificacoes",
        "paises",
        "motivos",
        "situacao_cadastral",
        "identificador_matriz_filial",
        "faixa_etaria",
        "porte",
        "tipo",
    ],
}


# Cobertura registrada no backend após a materialização. Tabelas históricas
# são particionadas por `data_referencia` (= `folder_date` da fonte);
# `simples` e `dicionario` são snapshot único (NonHistorical).
COVERAGE = {
    EMPRESAS_TABLE_ID: PartBdpro(
        date_column=DateOnly(col="data_referencia"),
        date_format=DateFormat.YEAR_MD,
    ),
    ESTABELECIMENTOS_TABLE_ID: PartBdpro(
        date_column=DateOnly(col="data_referencia"),
        date_format=DateFormat.YEAR_MD,
    ),
    SOCIOS_TABLE_ID: PartBdpro(
        date_column=DateOnly(col="data_referencia"),
        date_format=DateFormat.YEAR_MD,
    ),
    SIMPLES_TABLE_ID: NonHistorical(),
    DICIONARIO_TABLE_ID: NonHistorical(),
}

# Tabelas sem cobertura temporal: o polling compara a data de última
# modificação da fonte contra `Table.Update` (`compare_against="table_update"`),
# e não a competência (`folder_date`) contra `Coverage`.
NON_HISTORICAL_TABLES = (SIMPLES_TABLE_ID, DICIONARIO_TABLE_ID)

# Formato de `folder_date` na fonte (nome da pasta, ex.: "2026-09").
FOLDER_DATE_FORMAT = "%Y-%m"

# Cron de produção de cada `check_update`
# O dicionário roda depois das 4 tabelas porque
# `get_table_unique_keys` lê as tabelas materializadas em dev.
CHECK_UPDATE_CRON = {
    EMPRESAS_TABLE_ID: "0 6 * * *",
    SOCIOS_TABLE_ID: "0 7 * * *",
    SIMPLES_TABLE_ID: "0 8 * * *",
    ESTABELECIMENTOS_TABLE_ID: "0 9 * * *",
    DICIONARIO_TABLE_ID: "35 14 * * *",
}

# Tabela do diretório atualizada após a materialização de `estabelecimentos`.
DIRETORIO_EMPRESA_DATASET_ID = "br_bd_diretorios_brasil"
DIRETORIO_EMPRESA_TABLE_ID = "empresa"

COMPONENTS_SPECS = {
    "empresas": {
        "table_name": "Empresas",
        "segmentada": True,
        "dicionario": False,
        "manual": False,
    },
    "estabelecimentos": {
        "table_name": "Estabelecimentos",
        "segmentada": True,
        "dicionario": False,
        "manual": False,
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
        "manual": False,
    },
    "socios": {
        "table_name": "Socios",
        "segmentada": True,
        "dicionario": False,
        "manual": False,
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
        "relationships": [{"id_tabela": "empresas", "nome_coluna": "porte"}],
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
