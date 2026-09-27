#!/usr/bin/env python3
"""Emit a columns_json payload for mcp__databasis__bulk_upsert_columns.

Reads the architecture CSV (English descriptions + type/dictionary/FK/unit flags)
and attaches Portuguese and Spanish translations from TRANSLATIONS below, so
columns can be registered directly (no Google Sheet). Writes
code/columns_json/<table>.json.

Usage:
    python models/world_bis_property_prices/code/build_columns_json.py
"""

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCH = ROOT / "code" / "architecture"
OUT = ROOT / "code" / "columns_json"

# name -> (description_pt, description_es). English comes from the architecture CSV.
TRANSLATIONS = {
    "year": (
        "Ano de referência da observação",
        "Año de referencia de la observación",
    ),
    "quarter": (
        "Trimestre de referência da observação, de 1 a 4",
        "Trimestre de referencia de la observación, de 1 a 4",
    ),
    "country_id": (
        "Código ISO 3166-1 alfa-3 da economia de referência",
        "Código ISO 3166-1 alfa-3 de la economía de referencia",
    ),
    "reference_area_code": (
        "Código da área de referência do BIS conforme publicado",
        "Código del área de referencia del BIS según lo publicado",
    ),
    "reference_area_name": (
        "Nome da economia ou agregado de referência",
        "Nombre de la economía o agregado de referencia",
    ),
    "value_type": (
        "Se a série é nominal ou real (deflacionada por preços ao consumidor)",
        "Si la serie es nominal o real (deflactada por precios al consumidor)",
    ),
    "measure": (
        "Estatística reportada pela série",
        "Estadística reportada por la serie",
    ),
    "unit": (
        "Unidade do valor reportado conforme publicado pelo BIS",
        "Unidad del valor reportado según lo publicado por el BIS",
    ),
    "bis_series_key": (
        "Chave da série SDMX do BIS",
        "Clave de la serie SDMX del BIS",
    ),
    "value": (
        "Valor da observação, na unidade indicada pela coluna unit",
        "Valor de la observación, en la unidad indicada por la columna unit",
    ),
}


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    for csv_path in sorted(ARCH.glob("*.csv")):
        table = csv_path.stem
        with open(csv_path, newline="") as fh:
            rows = list(csv.DictReader(fh))
        cols = []
        for r in rows:
            pt, es = TRANSLATIONS[r["name"]]
            col = {
                "name": r["name"],
                "bigquery_type": r["bigquery_type"],
                "description_pt": pt,
                "description_en": r["description"],
                "description_es": es,
                "covered_by_dictionary": r["covered_by_dictionary"]
                .strip()
                .lower()
                == "yes",
                "has_sensitive_data": r["has_sensitive_data"].strip().lower()
                == "yes",
            }
            if r["directory_column"].strip():
                col["directory_column"] = r["directory_column"].strip()
            if r["measurement_unit"].strip():
                col["measurement_unit"] = r["measurement_unit"].strip()
            cols.append(col)
        (OUT / f"{table}.json").write_text(
            json.dumps(cols, ensure_ascii=False, indent=2)
        )
        print(
            f"{table}: {len(cols)} columns -> code/columns_json/{table}.json"
        )


if __name__ == "__main__":
    main()
