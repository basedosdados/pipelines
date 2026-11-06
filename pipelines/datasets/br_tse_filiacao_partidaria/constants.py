"""
Constantes de br_tse_filiacao_partidaria.
"""

from datetime import date
from enum import Enum

DATASET_ID = "br_tse_filiacao_partidaria"

MICRODADOS_TABLE_ID = "microdados"


class constants(Enum):
    """Constantes de br_tse_filiacao_partidaria."""

    PARTIDOS_URL = (
        "https://filia2-consulta.tse.jus.br/filia-consulta/rest/v1/partidos"
    )
    MUNICIPIOS_URL = (
        "https://filia2-consulta.tse.jus.br/filia-consulta/rest/v1"
        "/localidade/{uf_id}/municipios"
    )
    ZONAS_URL = (
        "https://filia2-consulta.tse.jus.br/filia-consulta/rest/v1"
        "/zona/municipio/{municipio}/zonasEleitorais"
    )
    FILIADOS_URL = (
        "https://filia2-consulta.tse.jus.br/filia-consulta/rest/v1"
        "/relacao-filiados?sgUe={uf}&cdMunicipio={municipio}"
        "&cdZona={zona}&sqPartido={partido}"
        "&currentPage={page}&pageSize={page_size}"
    )

    UF_IDS = list(range(1, 29))
    PAGE_SIZE = 10_000

    IMPERSONATE = "chrome"
    REQUEST_TIMEOUT = 120
    MAX_ATTEMPTS = 5
    RETRY_DELAY_SECONDS = 15
    MAX_WORKERS = 16
    CHUNK_SIZE = 1_000

    PATH = "/tmp/br_tse_filiacao_partidaria/"

    API_COLUMNS = [
        "sqRegistroFiliacao",
        "cdMotivoCancelamento",
        "cdMotivoDesfiliacao",
        "cdMunicipio",
        "codLocalidadeTse",
        "codObjetoZona",
        "codSitEleitor",
        "desSituacaoEleitor",
        "dtCancelamento",
        "dtDesfiliacao",
        "dtExclusao",
        "dtFiliacao",
        "tsCadastroDesfiliacao",
        "nmEleitor",
        "nmPartido",
        "nmSocialEleitor",
        "nomLocalidade",
        "nrLegenda",
        "nrTituloEleitor",
        "numZona",
        "sgPartido",
        "sqPartido",
        "stRegistroFiliacao",
        "tpSexo",
        "indPendencia",
        "numSecao",
        "indOrigem",
        "numCpf",
    ]
    EMPTY_COLUMNS = ["id_municipio", "id_municipio_tse"]

    DATE_COLUMNS = [
        "dtCancelamento",
        "dtDesfiliacao",
        "dtExclusao",
        "dtFiliacao",
        "tsCadastroDesfiliacao",
    ]
    MIN_DATE = date(1980, 1, 1)
    TITLE_CASE_COLUMNS = ["nmEleitor", "desSituacaoEleitor"]
    SEXO_LABELS = {"2": "Masculino", "4": "Feminino"}
