"""Emit bulk_upsert_columns payloads from the architecture CSVs.

    python gen_columns_json.py <table>

The architecture CSV is the source of truth, so the metadata registration reads it
rather than restating column facts by hand.
"""

import csv
import json
import sys
from pathlib import Path

ARCH = Path(__file__).resolve().parent / "architecture"


def payload(table: str) -> str:
    out = []
    with (ARCH / f"{table}.csv").open(encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            col = {
                "name": row["name"],
                "bigquery_type": row["bigquery_type"],
                "description_pt": row["description"],
                "description_en": row["description_en"],
                "description_es": row["description_es"],
                "covered_by_dictionary": row["covered_by_dictionary"] == "yes",
                "has_sensitive_data": row["has_sensitive_data"] == "yes",
            }
            if row["directory_column"]:
                col["directory_column"] = row["directory_column"]
            if row["measurement_unit"]:
                col["measurement_unit"] = row["measurement_unit"]
            if row["observations"]:
                col["observations"] = row["observations"]
            out.append(col)
    return json.dumps(out, ensure_ascii=False, separators=(",", ":"))


if __name__ == "__main__":
    print(payload(sys.argv[1]))
