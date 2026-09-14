"""Emit columns_json payloads for mcp__databasis__bulk_upsert_columns.

Reads the architecture CSVs (which already carry PT/EN/ES descriptions, type,
covered_by_dictionary, measurement_unit) and writes one JSON per table under
code/columns_json/. No Google Sheet needed. All columns are US-geography or
descriptive, so no directory_column links are set.

Run: uv run python models/us_usda_nass/code/build_columns_json.py
"""

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCH = ROOT / "code" / "architecture"
OUT = ROOT / "code" / "columns_json"

TABLES = [
    "survey_national",
    "survey_state",
    "survey_agricultural_district",
    "survey_county",
    "census_of_agriculture_national",
    "census_of_agriculture_state",
    "census_of_agriculture_county",
    "dicionario",
]


def build(table: str) -> list[dict]:
    cols = []
    with open(ARCH / f"{table}.csv", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            entry = {
                "name": r["name"],
                "bigquery_type": r["bigquery_type"],
                "description_pt": r["description"],
                "description_en": r["description_en"],
                "description_es": r["description_es"],
                "covered_by_dictionary": r["covered_by_dictionary"]
                .strip()
                .lower()
                == "yes",
                "has_sensitive_data": r["has_sensitive_data"].strip().lower()
                == "yes",
            }
            if r.get("measurement_unit", "").strip():
                entry["measurement_unit"] = r["measurement_unit"].strip()
            if r.get("observations", "").strip():
                entry["observations"] = r["observations"].strip()
            cols.append(entry)
    return cols


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for t in TABLES:
        cols = build(t)
        (OUT / f"{t}.json").write_text(
            json.dumps(cols, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"  wrote {t}.json ({len(cols)} columns)")


if __name__ == "__main__":
    main()
