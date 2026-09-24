"""Static configuration for the br_mj_sinesp onboarding and pipeline.

Single source of truth for URLs, label normalisation and the crime-type key
map. The recurring Prefect pipeline imports from here; nothing is duplicated.
"""

from __future__ import annotations

import functools
import os
import re
import unicodedata

DATASET_ID = "br_mj_sinesp"

BASE_URL = (
    "https://www.gov.br/mj/pt-br/assuntos/sua-seguranca/seguranca-publica/"
    "estatistica/download/dnsp-base-de-dados/bancovde-{year}.xlsx/@@download/file"
)
LANDING_PAGE = (
    "https://www.gov.br/mj/pt-br/assuntos/sua-seguranca/seguranca-publica/"
    "estatistica/dados_nacionais_de_seguranca_publica"
)
# gov.br rejects HEAD and bare user agents; a browser UA plus a referer is enough.
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    "Referer": LANDING_PAGE,
}

FIRST_YEAR = 2015

DATA_DIR = os.environ.get(
    "SINESP_DATA_DIR", os.path.expanduser("~/Downloads/br_mj_sinesp_data")
)
INPUT_DIR = os.path.join(DATA_DIR, "input")
OUTPUT_DIR = os.path.join(DATA_DIR, "output")
ARCH_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "architecture"
)

TABLE_MUNICIPIO = "municipio_mes"
TABLE_UF = "uf_mes"
TABLE_DICIONARIO = "dicionario"
TABLES = [TABLE_MUNICIPIO, TABLE_UF, TABLE_DICIONARIO]

# Source columns, in file order.
SOURCE_COLUMNS = [
    "uf",
    "municipio",
    "evento",
    "data_referencia",
    "agente",
    "arma",
    "faixa_etaria",
    "feminino",
    "masculino",
    "nao_informado",
    "total_vitima",
    "total",
    "total_peso",
    "abrangencia",
]

# The source writes this in `municipio` for series published only at state level.
UF_SENTINEL = {"NAO INFORMADO"}

# Created in 2024 and never an operating municipality in SINESP; it appears in
# the BD directory, so it must be excluded from any completeness benchmark or
# every year reads as one municipality short.
NON_OPERATING_MUNICIPIOS = {"5101837"}  # Boa Esperança do Norte/MT


def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c))


def normalise_name(s) -> str | None:
    """Accent-free, punctuation-free uppercase form, for municipality matching."""
    if s is None:
        return None
    s = strip_accents(str(s).strip()).upper()
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def slugify(s: str) -> str:
    s = strip_accents(str(s).strip()).lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")


# Crime-type keys. Derived mechanically from the source label by `slugify`,
# except where the mechanical form is unusable. The dicionario table records
# the exact raw label behind every key, per year, so this map is documentation
# rather than a hidden translation.
TIPO_OCORRENCIA_OVERRIDES = {
    "Roubo seguido de morte (latrocínio)": "latrocinio",
    "Mortes a esclarecer (sem indício de crime)": "morte_a_esclarecer",
    # Two distinct road-death series: the state indicator of Resolução 06, and
    # the federal highway police count. Keys must not collide.
    "Morte no trânsito ou em decorrência dele (exceto homicídio doloso)": (
        "morte_no_transito_exceto_homicidio_doloso"
    ),
    "Mortes no trânsito": "morte_no_transito",
    "Arma de Fogo Apreendida": "apreensao_de_arma_de_fogo",
    "Emissão de Alvarás de licença": "emissao_de_alvara_de_licenca",
}


@functools.cache
def tipo_ocorrencia_key(label: str) -> str:
    return TIPO_OCORRENCIA_OVERRIDES.get(label, slugify(label))


# Measure columns are mutually exclusive by crime type: a series carries either
# a victim count (with its sex split), an occurrence count, or a seized weight.
MEASURE_TOTAL = "total"
MEASURE_VITIMA = "total_vitima"
MEASURE_PESO = "total_peso"
