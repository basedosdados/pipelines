"""Build the columns_json payload for the two ComprasNet tables.

The architecture CSVs carry `observations` in Portuguese only. The backend
stores observations per language, and a language left empty is blank on the
site for those readers — which is how 3,022 production columns ended up
PT-only. The translations come from observation_translations, shared with
register_metadata, so the payload always carries all three.

    uv run --no-project python build_comprasnet_columns.py <table> > payload.json
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

ARCH = Path(__file__).resolve().parent / "architecture"

# The translations moved to observation_translations.py, which register_metadata
# also reads, so one map now covers every table in the dataset and
# check_translations() polices all of them. Keeping a second copy here is how
# the two drift apart.
from observation_translations import OBSERVATIONS  # noqa: E402


def build(table: str) -> list[dict]:
    rows = []
    with (ARCH / f"{table}.csv").open(encoding="utf-8") as handle:
        for raw in csv.DictReader(handle):
            column = {
                "name": raw["name"],
                "bigquery_type": raw["bigquery_type"],
                "description_pt": raw["description"],
                "description_en": raw["description_en"],
                "description_es": raw["description_es"],
                "covered_by_dictionary": raw["covered_by_dictionary"].strip()
                == "yes",
                "has_sensitive_data": raw["has_sensitive_data"].strip()
                == "yes",
            }
            if raw["measurement_unit"].strip():
                column["measurement_unit"] = raw["measurement_unit"].strip()
            if raw["directory_column"].strip():
                column["directory_column"] = raw["directory_column"].strip()
            note = raw["observations"].strip()
            if note:
                if note not in OBSERVATIONS:
                    raise SystemExit(
                        f"untranslated observation on {raw['name']}: {note!r}"
                    )
                english, spanish = OBSERVATIONS[note]
                column["observations_pt"] = note
                column["observations_en"] = english
                column["observations_es"] = spanish
            rows.append(column)
    return rows


if __name__ == "__main__":
    print(json.dumps(build(sys.argv[1]), ensure_ascii=False))
