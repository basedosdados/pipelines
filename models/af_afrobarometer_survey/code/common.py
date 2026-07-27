#!/usr/bin/env python3
"""Shared configuration and helpers for the af_afrobarometer_survey pipeline.

Covers: the 9 merged-round source files, BigQuery-column-name sanitisation,
SPSS-format -> BigQuery-type inference, and a country-name -> ISO3 map used to
derive the `country_iso3_code` directory column.
"""

from __future__ import annotations

import re
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INPUT_DIR = ROOT / "input"
OUTPUT_DIR = ROOT / "output"
ARCH_DIR = Path(__file__).resolve().parent / "architecture"
META_CACHE = Path(__file__).resolve().parent / "meta_cache.json"

# Merged round files. `num` is the round; `filename` is the cached local name.
ROUNDS = [
    {
        "num": 1,
        "slug": "round1",
        "filename": "merged_r1_data.sav",
        "url": "https://www.afrobarometer.org/wp-content/uploads/2022/02/merged_r1_data.sav",
    },
    {
        "num": 2,
        "slug": "round2",
        "filename": "merged_r2_data.sav",
        "url": "https://www.afrobarometer.org/wp-content/uploads/2022/02/merged_r2_data.sav",
    },
    {
        "num": 3,
        "slug": "round3",
        "filename": "merged_r3_data.sav",
        "url": "https://www.afrobarometer.org/wp-content/uploads/2022/02/merged_r3_data.sav",
    },
    {
        "num": 4,
        "slug": "round4",
        "filename": "merged_r4_data.sav",
        "url": "https://www.afrobarometer.org/wp-content/uploads/2022/02/merged_r4_data.sav",
    },
    {
        "num": 5,
        "slug": "round5",
        "filename": "merged_r5_data.sav",
        "url": "https://www.afrobarometer.org/wp-content/uploads/2022/02/merged-round-5-data-34-countries-2011-2013-last-update-july-2015_0.sav",
    },
    {
        "num": 6,
        "slug": "round6",
        "filename": "merged_r6_data.sav",
        "url": "https://www.afrobarometer.org/wp-content/uploads/2022/02/merged_r6_data_2016_36countries2.sav",
    },
    {
        "num": 7,
        "slug": "round7",
        "filename": "merged_r7_data.sav",
        "url": "https://www.afrobarometer.org/wp-content/uploads/2022/02/r7_merged_data_34ctry.release.sav",
    },
    {
        "num": 8,
        "slug": "round8",
        "filename": "merged_r8_data.sav",
        "url": "https://www.afrobarometer.org/wp-content/uploads/2023/03/afrobarometer_release-dataset_merge-34ctry_r8_en_2023-03-01.sav",
    },
    {
        "num": 9,
        "slug": "round9",
        "filename": "merged_r9_data.sav",
        "url": "https://www.afrobarometer.org/wp-content/uploads/2025/06/R9.Merge_39ctry.20Nov23.final_.release_Updated.4Jun25-3.sav",
    },
]

DATASET_ID = "af_afrobarometer_survey"

# --------------------------------------------------------------------------
# BigQuery column-name sanitisation
# --------------------------------------------------------------------------


def clean_name(name: str) -> str:
    """Lowercase and coerce an SPSS variable name to a valid BQ column name.

    BigQuery columns must match [A-Za-z_][A-Za-z0-9_]*; Afrobarometer names
    contain '.' and '-' (e.g. COUNTRY.old.spelling, EA_FAC_F2). Non-word chars
    become '_', runs collapse, and a leading digit gets a 'v' prefix.
    """
    n = name.strip().lower()
    n = re.sub(r"[^0-9a-z_]+", "_", n)
    n = re.sub(r"_+", "_", n).strip("_")
    if not n:
        n = "col"
    if n[0].isdigit():
        n = "v" + n
    return n


# --------------------------------------------------------------------------
# SPSS print-format -> BigQuery type inference
# --------------------------------------------------------------------------

_DATE_PREFIXES = (
    "DATE",
    "ADATE",
    "EDATE",
    "JDATE",
    "SDATE",
    "DATETIME",
    "YMDHMS",
)
_TIME_PREFIXES = ("TIME", "DTIME")


def infer_bq_type(spss_fmt: str, is_integral: bool) -> str:
    """Map an SPSS print format to a BigQuery type.

    `is_integral` is whether every non-null numeric value equals its integer
    part (only consulted for numeric formats). Date/time formats are decided
    from the format string; strings ('A...') map to STRING.
    """
    fmt = (spss_fmt or "").upper().strip()
    if fmt.startswith("A"):
        return "STRING"
    if any(fmt.startswith(p) for p in _DATE_PREFIXES):
        return "DATE"
    if any(fmt.startswith(p) for p in _TIME_PREFIXES):
        return "STRING"  # times stored as HH:MM:SS strings
    # numeric family: F, COMMA, DOT, PCT, E, N, Z, DOLLAR, CCA...
    m = re.search(r"\.(\d+)$", fmt)
    decimals = int(m.group(1)) if m else 0
    if decimals == 0 and is_integral:
        return "INT64"
    return "FLOAT64"


# --------------------------------------------------------------------------
# Country name -> ISO3 (for the country_iso3_code directory column)
# --------------------------------------------------------------------------


def _norm(s: str) -> str:
    # strip accents (e-acute -> e) but keep base letters, then turn every
    # non-alphanumeric char (spaces, hyphens, U+2019 or ASCII apostrophes) into
    # a single space so both spellings of Cote d Ivoire collapse the same way.
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return s


# keys are normalised country labels; every distinct COUNTRY value label across
# all nine rounds must resolve here (clean_data asserts full coverage).
_ISO3_RAW = {
    "algeria": "DZA",
    "angola": "AGO",
    "benin": "BEN",
    "botswana": "BWA",
    "burkina faso": "BFA",
    "burundi": "BDI",
    "cameroon": "CMR",
    "cape verde": "CPV",
    "cabo verde": "CPV",
    "congo brazzaville": "COG",
    "congo republic": "COG",
    "republic of congo": "COG",
    "republic of the congo": "COG",
    "cote d ivoire": "CIV",
    "ivory coast": "CIV",
    "egypt": "EGY",
    "eswatini": "SWZ",
    "swaziland": "SWZ",
    "ethiopia": "ETH",
    "gabon": "GAB",
    "gambia": "GMB",
    "the gambia": "GMB",
    "ghana": "GHA",
    "guinea": "GIN",
    "kenya": "KEN",
    "lesotho": "LSO",
    "liberia": "LBR",
    "madagascar": "MDG",
    "malawi": "MWI",
    "mali": "MLI",
    "mauritania": "MRT",
    "mauritius": "MUS",
    "morocco": "MAR",
    "mozambique": "MOZ",
    "namibia": "NAM",
    "niger": "NER",
    "nigeria": "NGA",
    "sao tome and principe": "STP",
    "senegal": "SEN",
    "seychelles": "SYC",
    "sierra leone": "SLE",
    "south africa": "ZAF",
    "sudan": "SDN",
    "tanzania": "TZA",
    "togo": "TGO",
    "tunisia": "TUN",
    "uganda": "UGA",
    "zambia": "ZMB",
    "zimbabwe": "ZWE",
}


def country_to_iso3(label: str) -> str | None:
    """Return ISO3 for a COUNTRY value label, or None if unmapped."""
    if label is None:
        return None
    return _ISO3_RAW.get(_norm(label))
