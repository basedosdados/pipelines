"""Generate the au_aec_elections architecture CSVs and the columns JSON.

The architecture is derived from ``pipelines/datasets/au_aec_elections/schema.py`` so
the sheets, the cleaning transform and the dbt models cannot drift apart.

Run:  uv run python models/au_aec_elections/code/build_architecture.py
"""

from __future__ import annotations

import csv
import json

from pipelines.datasets.au_aec_elections import schema
from pipelines.datasets.au_aec_elections.constants import (
    ARCHITECTURE_DIR,
    constants,
)

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


def main() -> None:
    ARCHITECTURE_DIR.mkdir(parents=True, exist_ok=True)
    columns_json: dict[str, list[dict]] = {}

    for table in constants.TABLES.value:
        cols = schema.TABLES[table]
        path = ARCHITECTURE_DIR / f"{table}.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            # csv.writer defaults to CRLF; force LF so regenerating does not
            # reintroduce mixed line endings that pre-commit then has to fix.
            writer = csv.writer(fh, lineterminator="\n")
            writer.writerow(HEADER)
            for c in cols:
                writer.writerow(
                    [
                        c.name,
                        c.bigquery_type,
                        c.description,
                        c.temporal_coverage,
                        c.covered_by_dictionary,
                        c.directory_column,
                        c.measurement_unit,
                        c.has_sensitive_data,
                        c.observations,
                        c.original_name,
                        c.description_en,
                        c.description_es,
                    ]
                )
        columns_json[table] = [
            {
                "name": c.name,
                "bigquery_type": c.bigquery_type,
                "description_pt": c.description,
                "description_en": c.description_en,
                "description_es": c.description_es,
                "measurement_unit": c.measurement_unit or None,
                "covered_by_dictionary": c.covered_by_dictionary == "yes",
                "directory_column": c.directory_column or None,
                "has_sensitive_data": c.has_sensitive_data == "yes",
                "observations": c.observations or None,
                "is_partition": c.name in schema.PARTITION_COLUMNS[table],
            }
            for c in cols
        ]
        print(f"{table:46s} {len(cols):3d} columns -> {path.name}")

    json_path = ARCHITECTURE_DIR / "columns.json"
    json_path.write_text(
        json.dumps(columns_json, ensure_ascii=False, indent=2), "utf-8"
    )
    total = sum(len(v) for v in columns_json.values())
    print(f"\n{len(columns_json)} tables, {total} columns -> {json_path.name}")


if __name__ == "__main__":
    main()
