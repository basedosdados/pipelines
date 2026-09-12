"""Write the bulk_upsert_columns payload for each table, from the architecture CSVs.

The backend metadata is generated from the same CSVs as the transform and the dbt
models, so the three cannot describe different schemas.

Run: uv run python models/us_nih_reporter/code/gen_columns_json.py [out_dir]
"""

import json
import sys
from pathlib import Path

from common import ALL_TABLES, load_cols


def payload(table: str) -> list[dict]:
    """Build one table's bulk_upsert_columns payload from its architecture CSV.

    Optional keys are omitted rather than sent empty: bulk_upsert_columns writes
    only the fields present for a row, so an empty string would blank a value
    that is already correct on a re-run.

    Args:
        table: Clean table slug, matching a sheet_<table>.csv.

    Returns:
        One dict per column, in the architecture's column order.
    """
    out = []
    for c in load_cols(table):
        entry = {
            "name": c.name,
            "bigquery_type": c.bq_type,
            "description_pt": c.description_pt,
            "description_en": c.description_en,
            "description_es": c.description_es,
            "covered_by_dictionary": c.covered_by_dictionary,
            "has_sensitive_data": c.has_sensitive_data,
        }
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
    """Write one columns_<table>.json per table into the output directory.

    Args:
        None. The first command-line argument is the output directory,
        defaulting to the working directory.

    Returns:
        None. Writes one JSON file per table and prints each path.
    """
    out_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")
    out_dir.mkdir(parents=True, exist_ok=True)
    for table in ALL_TABLES:
        cols = payload(table)
        path = out_dir / f"columns_{table}.json"
        path.write_text(json.dumps(cols, ensure_ascii=False, indent=1))
        print(f"wrote {path} ({len(cols)} columns)")


if __name__ == "__main__":
    main()
