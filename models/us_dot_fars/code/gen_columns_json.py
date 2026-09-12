"""Write the bulk_upsert_columns payload for each table, from the architecture CSVs.

The backend metadata is generated from the same CSVs as the transform and the dbt
models, so the three cannot describe different schemas.

Run: uv run python models/us_dot_fars/code/gen_columns_json.py [out_dir]
"""

import json
import sys
from pathlib import Path

from common import ALL_TABLES, load_cols


def payload(table: str) -> list[dict]:
    out = []
    for c in load_cols(table):
        entry = {
            "name": c.name,
            "bigquery_type": c.bq_type,
            "description_pt": c.description_pt,
            "description_en": c.description_en,
            "description_es": c.description_es,
            "covered_by_dictionary": "yes"
            if c.covered_by_dictionary
            else "no",
            "has_sensitive_data": "yes" if c.has_sensitive_data else "no",
        }
        if c.temporal_coverage:
            entry["temporal_coverage"] = c.temporal_coverage
        if c.directory_column:
            entry["directory_column"] = c.directory_column
        if c.measurement_unit:
            entry["measurement_unit"] = c.measurement_unit
        if c.observations_pt:
            entry["observations_pt"] = c.observations_pt
            entry["observations_en"] = c.observations_en
            entry["observations_es"] = c.observations_es
        out.append(entry)
    return out


def main() -> None:
    out_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")
    out_dir.mkdir(parents=True, exist_ok=True)
    for table in ALL_TABLES:
        cols = payload(table)
        path = out_dir / f"columns_{table}.json"
        path.write_text(json.dumps(cols, ensure_ascii=False, indent=1))
        print(f"wrote {path} ({len(cols)} columns)")


if __name__ == "__main__":
    main()
