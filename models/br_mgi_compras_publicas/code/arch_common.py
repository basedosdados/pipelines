"""Shared helpers for building br_mgi_compras_publicas architecture CSVs.

One row per column. `c()` keeps the long per-column specs readable: everything
except the name, type and Portuguese description has a sensible default.
"""

from __future__ import annotations

import csv
from pathlib import Path

HEADER = [
    "name",
    "bigquery_type",
    "description",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "original_name",
    "description_en",
    "description_es",
]

# Directory foreign keys used across the dataset.
DIR_ANO = "br_bd_diretorios_data_tempo.ano:ano"
DIR_UF = "br_bd_diretorios_brasil.uf:sigla_uf"
DIR_MUNICIPIO = "br_bd_diretorios_brasil.municipio:id_municipio"

BRL = "BRL"


def c(
    name: str,
    bq_type: str,
    desc: str,
    *,
    en: str = "",
    es: str = "",
    dic: bool = False,
    directory: str = "",
    unit: str = "",
    sensitive: bool = False,
    obs: str = "",
    original: str = "",
    coverage: str = "",
) -> dict:
    """Build one architecture row.

    Column descriptions must not end with a period, must start with a capital
    letter, and exist in all three languages -- see `.claude/rules/data-basis-style.md`.
    """
    for label, text in (("pt", desc), ("en", en), ("es", es)):
        if text and text.rstrip().endswith("."):
            raise ValueError(
                f"{name} [{label}]: column description must not end with a period"
            )
        if text and not text[0].isupper():
            raise ValueError(
                f"{name} [{label}]: column description must start with a capital letter"
            )
    if bq_type in ("INT64", "FLOAT64") and not unit:
        raise ValueError(f"{name}: numeric column needs a measurement_unit")
    if dic and bq_type != "STRING":
        raise ValueError(f"{name}: covered_by_dictionary requires STRING")
    return {
        "name": name,
        "bigquery_type": bq_type,
        "description": desc,
        "temporal_coverage": coverage,
        "covered_by_dictionary": "yes" if dic else "no",
        "directory_column": directory,
        "measurement_unit": unit,
        "has_sensitive_data": "yes" if sensitive else "no",
        "observations": obs,
        "original_name": original,
        "description_en": en,
        "description_es": es,
    }


def write(table: str, columns: list[dict], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    names = [col["name"] for col in columns]
    if len(names) != len(set(names)):
        dupes = sorted({n for n in names if names.count(n) > 1})
        raise ValueError(f"{table}: duplicate column names {dupes}")
    path = out_dir / f"{table}.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADER, lineterminator="\n")
        writer.writeheader()
        writer.writerows(columns)
    print(f"  {table:<32} {len(columns):>3} columns -> {path.name}")
