"""
Constantes de br_stn_capag.
"""

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
# Formatos abaixo são aceitos (a API vem com formatos como PDF também)
DATA_FORMATS = ("CSV", "XLSX")

HTTP_TIMEOUT = 60

DOWNLOAD_TIMEOUT = 5 * 60
DOWNLOAD_CHUNK_SIZE = 1024 * 1024
# O servidor responde 429 a downloads em sequência: espera entre arquivos
# e tentativas com espera crescente (ou o `Retry-After`, se vier).
DOWNLOAD_PAUSE_SECONDS = 2
DOWNLOAD_MAX_ATTEMPTS = 5
DOWNLOAD_BACKOFF_SECONDS = 10
RETRY_STATUS_CODES = (429, 502, 503, 504)
