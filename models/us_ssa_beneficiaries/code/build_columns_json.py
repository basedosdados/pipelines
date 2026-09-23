"""Turn the architecture CSVs into columns_json payloads for bulk_upsert_columns."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

ARCH = Path(__file__).resolve().parent / "architecture"


def columns_json(table: str) -> str:
    """Return the bulk_upsert_columns payload for one table, in architecture order."""
    arch = pd.read_csv(ARCH / f"{table}.csv", dtype=str).fillna("")
    out = []
    for _, r in arch.iterrows():
        entry = {
            "name": r["name"],
            "bigquery_type": r["bigquery_type"],
            "description_pt": r["description"],
            "description_en": r["description_en"],
            "description_es": r["description_es"],
            "covered_by_dictionary": r["covered_by_dictionary"] == "yes",
            "has_sensitive_data": r["has_sensitive_data"] == "yes",
        }
        if r["directory_column"]:
            entry["directory_column"] = r["directory_column"]
        if r["measurement_unit"]:
            entry["measurement_unit"] = r["measurement_unit"]
        if r["observations"]:
            # Observations are per-language; leaving EN/ES blank is how
            # thousands of production columns ended up Portuguese-only.
            entry["observations_pt"] = r["observations"]
            entry["observations_en"] = r["observations"]
            entry["observations_es"] = r["observations"]
        out.append(entry)
    return json.dumps(out, ensure_ascii=False)


if __name__ == "__main__":
    for path in sorted(ARCH.glob("*.csv")):
        payload = columns_json(path.stem)
        print(
            f"{path.stem}: {len(json.loads(payload))} columns, {len(payload)} bytes"
        )
