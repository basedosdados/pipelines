"""
Constantes de br_stn_capag.
"""

from pipelines.utils.metadata.domain import AllFree, DateFormat, DateOnly

MUNICIPIOS_TABLE_ID = "municipios"
ESTADOS_TABLE_ID = "estados"

DATASET_ID = "br_stn_capag"

PACKAGE_SHOW_URL = (
    "https://www.tesourotransparente.gov.br/ckan/api/3/action/package_show"
)

CKAN_PACKAGES = {
    MUNICIPIOS_TABLE_ID: "capag-municipios",
    ESTADOS_TABLE_ID: "capag-estados",
}

DATA_FORMATS = ("CSV", "XLSX")  # Formatos aceitos

HTTP_TIMEOUT = 60
DOWNLOAD_TIMEOUT = 5 * 60
DOWNLOAD_CHUNK_SIZE = 1024 * 1024

# O servidor responde 429 a downloads em sequência: espera entre arquivos
# e tentativas com espera crescente (ou o `Retry-After`, se vier).
DOWNLOAD_PAUSE_SECONDS = 2
DOWNLOAD_MAX_ATTEMPTS = 5
DOWNLOAD_BACKOFF_SECONDS = 10
RETRY_STATUS_CODES = (429, 502, 503, 504)


HEADER_SCAN_ROWS = 15  # Quantas linhas varrer atrás do cabeçalho
PREFERRED_SHEETS = (
    "previa_da_capag",
)  # Abas lidas primeiro, pelo nome normalizado


NULL_TOKENS = ("", "-", "n.d.", "n.d", "nd", "n.e.", "n.e", "ne")

# Colunas correspondentes a indicadores
_INDICADORES = {
    "indicador_1": "indicador_endividamento",
    "nota_1": "nota_endividamento",
    "indicador_2": "indicador_poupanca_corrente",
    "nota_2": "nota_poupanca_corrente",
    "indicador_3": "indicador_liquidez",
    "nota_3": "nota_liquidez",
    "capag": "nota_capag",
    "capag_2022": "nota_capag",
    "capag_oficial": "nota_capag",
    "classificacao_capag": "nota_capag",
    "classificacao_da_capag": "nota_capag",
}

COLUNAS_FONTE = {
    MUNICIPIOS_TABLE_ID: {
        "cod_ibge": "id_municipio",
        "codigo_municipio_completo": "id_municipio",
        "uf": "sigla_uf",
        "populacao": "populacao",
        **_INDICADORES,
    },
    ESTADOS_TABLE_ID: {"uf": "sigla_uf", **_INDICADORES},
}

# Colunas que o cabeçalho precisa ter.
COLUNAS_OBRIGATORIAS = {
    MUNICIPIOS_TABLE_ID: ("id_municipio", "sigla_uf", "nota_capag"),
    ESTADOS_TABLE_ID: ("sigla_uf", "nota_capag"),
}

COLUNAS_SAIDA = {
    MUNICIPIOS_TABLE_ID: (
        "id_municipio",
        "sigla_uf",
        "populacao",
        "indicador_endividamento",
        "nota_endividamento",
        "indicador_poupanca_corrente",
        "nota_poupanca_corrente",
        "indicador_liquidez",
        "nota_liquidez",
        "nota_capag",
        "data_referencia",
    ),
    ESTADOS_TABLE_ID: (
        "sigla_uf",
        "indicador_endividamento",
        "nota_endividamento",
        "indicador_poupanca_corrente",
        "nota_poupanca_corrente",
        "indicador_liquidez",
        "nota_liquidez",
        "nota_capag",
        "data_referencia",
    ),
}

COLUNAS_NUMERICAS = (
    "indicador_endividamento",
    "indicador_poupanca_corrente",
    "indicador_liquidez",
)

# Mínimo de linhas por recurso
MIN_ROWS = {MUNICIPIOS_TABLE_ID: 5000, ESTADOS_TABLE_ID: 26}
CHAVE_UNICA = {
    MUNICIPIOS_TABLE_ID: ("id_municipio",),
    ESTADOS_TABLE_ID: ("sigla_uf",),
}

COVERAGE = AllFree(
    date_column=DateOnly(col="data_referencia"),
    date_format=DateFormat.YEAR_MD,
)
