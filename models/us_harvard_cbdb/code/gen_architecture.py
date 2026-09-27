"""Generate architecture CSVs (one per table) from schema_spec.

BD architecture columns:
  name, bigquery_type, description, temporal_coverage, covered_by_dictionary,
  directory_column, measurement_unit, has_sensitive_data, observations, original_name

Intra-dataset references (ref='<sibling>') are NOT BD directories, so
directory_column stays blank and the sibling ref is recorded in observations.
"""

import csv
import os

# pyrefly: ignore [missing-import]
from schema_spec import TABLE_ORDER, TABLES

OUT = os.path.join(os.path.dirname(__file__), "architecture")
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


def main():
    os.makedirs(OUT, exist_ok=True)
    for t in TABLE_ORDER:
        spec = TABLES[t]
        path = os.path.join(OUT, f"{t}.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(HEADER)
            for c in spec["columns"]:
                ref = c.get("ref", "")
                obs = ""
                if ref == "__person":
                    obs = "References us_harvard_cbdb.person.person_id"
                elif ref:
                    obs = f"References sibling table us_harvard_cbdb.{ref}"
                w.writerow(
                    [
                        c["name"],
                        c["type"],
                        c["pt"],
                        "",
                        c.get("dict", "no"),
                        "",
                        c.get("unit", ""),
                        "no",
                        obs,
                        c.get("src") or "",
                    ]
                )
        print(f"wrote {path} ({len(spec['columns'])} cols)")


if __name__ == "__main__":
    main()
