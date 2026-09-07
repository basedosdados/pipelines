"""Write the architecture CSV for every table from the shared specification.

The architecture table is the source of truth for column names, types, order and
descriptions. Generating it from ``spec.py`` rather than editing it by hand is
what keeps the dbt models, the parquet column order and the backend metadata in
agreement.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_fbi_cde.spec import (
    ARCHITECTURE_HEADER,
    TABLES,
)

OUT = Path(__file__).resolve().parent / "architecture"


def temporal_coverage(table, spec):
    if spec["first_year"] is None:
        return ""
    return f"{spec['first_year']}(1){spec['last_year']}"


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    for table, spec in TABLES.items():
        path = OUT / f"{table}.csv"
        with open(path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(ARCHITECTURE_HEADER)
            for column in spec["columns"]:
                writer.writerow(
                    [
                        column["name"],
                        column["bigquery_type"],
                        column["description_pt"],
                        "",
                        column["covered_by_dictionary"],
                        column["directory_column"],
                        column["measurement_unit"],
                        "no",
                        column["observations"],
                        column["original_name"],
                    ]
                )
        print(f"{table:32s} {len(spec['columns']):>3} columns -> {path.name}")

    # Guard rails the house conventions require, checked here so a bad column
    # cannot reach BigQuery.
    problems = []
    for table, spec in TABLES.items():
        for column in spec["columns"]:
            numeric = column["bigquery_type"] in ("INT64", "FLOAT64")
            if numeric and not column["measurement_unit"]:
                problems.append(
                    f"{table}.{column['name']}: numeric without a measurement unit"
                )
            if (
                column["covered_by_dictionary"] == "yes"
                and column["bigquery_type"] != "STRING"
            ):
                problems.append(
                    f"{table}.{column['name']}: dictionary-covered but not STRING"
                )
            for language in ("pt", "en", "es"):
                text = column[f"description_{language}"]
                if not text:
                    problems.append(
                        f"{table}.{column['name']}: missing {language} description"
                    )
                elif text.endswith("."):
                    problems.append(
                        f"{table}.{column['name']}: {language} description ends in a period"
                    )
                elif text[0] != text[0].upper():
                    problems.append(
                        f"{table}.{column['name']}: {language} description not capitalised"
                    )
    if problems:
        print("\nSTYLE PROBLEMS")
        for problem in problems:
            print("  ", problem)
        raise SystemExit(1)
    print("\nall columns pass the type, unit and description checks")


if __name__ == "__main__":
    main()
