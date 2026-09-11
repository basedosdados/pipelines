#!/usr/bin/env python3
"""Emit columns_json payloads for the 12 Detailed-release tables.

Companion to `build_columns_json.py`, which does the same for the four Tier-1
tables. Both feed `mcp__databasis__bulk_upsert_columns`, which registers types,
directory links and measurement units without needing a Google Sheet (the sheet
path drops description_en for English-source datasets).

The three languages come straight from `architecture_detailed.py`, which holds
the Portuguese and Spanish alongside the English rather than in a side table.

Writes `code/columns_json_detailed/<table>.json`.

Usage:
    uv run python models/au_abs_labour_force/code/build_columns_json_detailed.py
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from architecture_detailed import TABLES, field

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "code" / "columns_json_detailed"


def payload(table: str) -> list[dict[str, object]]:
    cols: list[dict[str, object]] = []
    for key in TABLES[table]:
        col: dict[str, object] = {
            "name": field(table, key, "name"),
            "bigquery_type": field(table, key, "bigquery_type"),
            "description_pt": field(table, key, "description_pt"),
            "description_en": field(table, key, "description"),
            "description_es": field(table, key, "description_es"),
            "covered_by_dictionary": field(table, key, "covered_by_dictionary")
            == "yes",
            "has_sensitive_data": field(table, key, "has_sensitive_data")
            == "yes",
        }
        if field(table, key, "directory_column"):
            col["directory_column"] = field(table, key, "directory_column")
        if field(table, key, "measurement_unit"):
            col["measurement_unit"] = field(table, key, "measurement_unit")
        cols.append(col)
    return cols


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for table in TABLES:
        p = OUT / f"{table}.json"
        p.write_text(json.dumps(payload(table), ensure_ascii=False, indent=2))
        print(f"wrote {p}  ({len(TABLES[table])} columns)")


if __name__ == "__main__":
    main()
