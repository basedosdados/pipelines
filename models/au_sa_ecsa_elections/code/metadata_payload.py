"""Emit the column payloads the backend registration reads.

One JSON file per table, in the shape ``bulk_upsert_columns(columns_json=...)``
expects, generated from the schema module so the backend records cannot drift
from the architecture CSVs or the dbt models.

Usage::

    PYTHONPATH=. python models/au_sa_ecsa_elections/code/metadata_payload.py
"""

from __future__ import annotations

import json
import pathlib

from pipelines.datasets.au_sa_ecsa_elections.schema import (
    OBSERVATION_TRANSLATIONS,
    TABLE_META,
    TABLES,
)

OUT = pathlib.Path(__file__).resolve().parent / "metadata"

# The directory foreign key as the backend spells it: the dataset's backend slug,
# not its GCP id.
DIRECTORY_SLUG = {
    "br_bd_diretorios_data_tempo.ano:ano": "diretorios_data_tempo.ano:ano",
    "br_bd_diretorios_au.state_electoral_division_2021:id_state_electoral_division": (
        "diretorios_au.state_electoral_division_2021:id_state_electoral_division"
    ),
}


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    summary = {}
    for table, columns in TABLES.items():
        payload = []
        for column in columns:
            entry: dict[str, object] = {
                "name": column.name,
                "bigquery_type": column.bigquery_type,
                "description_pt": column.description,
                "description_en": column.description_en,
                "description_es": column.description_es,
                "covered_by_dictionary": column.covered_by_dictionary == "yes",
                "has_sensitive_data": column.has_sensitive_data == "yes",
            }
            if column.measurement_unit:
                entry["measurement_unit"] = column.measurement_unit
            if column.directory_column:
                entry["directory_column"] = DIRECTORY_SLUG[
                    column.directory_column
                ]
            if column.observations:
                en, es = OBSERVATION_TRANSLATIONS[column.observations]
                entry["observations_pt"] = column.observations
                entry["observations_en"] = en
                entry["observations_es"] = es
            payload.append(entry)
        (OUT / f"{table}.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=1)
        )
        meta = TABLE_META[table]
        summary[table] = {
            "columns": len(payload),
            "name_pt": meta.name_pt,
            "name_en": meta.name_en,
            "name_es": meta.name_es,
            "description_pt": meta.description_pt,
            "description_en": meta.description_en,
            "description_es": meta.description_es,
            "observation_levels": meta.observation_levels,
        }
    (OUT / "tables.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=1)
    )
    print(f"wrote {len(TABLES)} column payloads and tables.json to {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
