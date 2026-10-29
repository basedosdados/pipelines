"""Write the architecture CSVs for fr_colibre_decp (one per table).

Usage (from the repo root):
    python -m models.fr_colibre_decp.code.build_architecture

The CSVs under ``architecture/`` are the source of truth for column order, types
and descriptions. The cleaning code and the dbt models follow them.
"""

import csv
from pathlib import Path

from models.fr_colibre_decp.code.architecture_spec import TABLES

OUT = Path(__file__).parent / "architecture"
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
]


def main() -> None:
    OUT.mkdir(exist_ok=True)
    for table, cols in TABLES.items():
        names = [c.name for c in cols]
        if len(names) != len(set(names)):
            raise ValueError(f"duplicate column names in {table}")
        path = OUT / f"{table}.csv"
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(HEADER)
            for c in cols:
                writer.writerow(
                    [
                        c.name,
                        c.bigquery_type,
                        c.pt,
                        "",
                        c.covered_by_dictionary,
                        c.directory_column,
                        c.measurement_unit,
                        c.has_sensitive_data,
                        c.observations,
                        c.original_name,
                    ]
                )
        print(f"wrote {path.name} ({len(cols)} columns)")


if __name__ == "__main__":
    main()
