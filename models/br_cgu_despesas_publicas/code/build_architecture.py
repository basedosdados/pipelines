"""Generate the architecture CSVs for br_cgu_despesas_publicas.

Usage:
    uv run python models/br_cgu_despesas_publicas/code/build_architecture.py

Reads the per-table specs via ``_specs.py`` and writes
``architecture/<table>.csv``. ``build_columns_json.py`` writes the trilingual
backend payload from the same specs, so the two artifacts cannot drift.

Note the explicit ``lineterminator="\\n"``: ``csv.writer`` defaults to ``\\r\\n``,
which the repo's mixed-line-ending pre-commit hook rewrites on every commit.
"""

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

# _specs.py is a sibling module reached through the sys.path insert above,
# which pyrefly cannot follow when it checks the project as a whole.
# pyrefly: ignore [missing-import]
from _specs import TABLES

OUT_DIR = Path(__file__).resolve().parent / "architecture"
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
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for table, (cols, period_column) in TABLES.items():
        path = OUT_DIR / f"{table}.csv"
        with open(path, "w", encoding="utf-8", newline="") as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow(HEADER)
            for c in cols:
                w.writerow(
                    [
                        c["name"],
                        c["bigquery_type"],
                        c["description_pt"],
                        "",
                        "no",
                        c["directory_column"],
                        c["measurement_unit"],
                        c["has_sensitive_data"],
                        c["observations_pt"],
                        c["source_header"] or period_column,
                    ]
                )
        print(f"wrote {path} ({len(cols)} columns)")


if __name__ == "__main__":
    main()
