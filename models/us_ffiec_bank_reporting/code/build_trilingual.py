"""Render architecture_trilingual/ -- the architecture CSVs plus PT/EN/ES columns.

register_metadata.py reads these; the plain architecture/ CSVs keep the single
Portuguese-slot `description` the Data Basis architecture schema defines.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

# The shared transform lives in the pipelines package so the recurring flow and
# this one-shot onboarding script use exactly one implementation.
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from translations import DESCRIPTIONS

from pipelines.datasets.us_ffiec_bank_reporting.common import CODE_DIR
from pipelines.datasets.us_ffiec_bank_reporting.schema_def import TABLES

OUT_DIR = CODE_DIR / "architecture_trilingual"

HEADER = [
    "name",
    "bigquery_type",
    "description_pt",
    "description_en",
    "description_es",
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
    missing = []
    for table, cols in TABLES.items():
        rows = []
        for name, typ, desc, dic, directory, unit, obs, original in cols:
            if desc not in DESCRIPTIONS:
                missing.append((table, name))
                continue
            pt, es = DESCRIPTIONS[desc]
            rows.append(
                [
                    name,
                    typ,
                    pt,
                    desc,
                    es,
                    "",
                    dic,
                    directory,
                    unit,
                    "no",
                    obs,
                    original,
                ]
            )
        path = OUT_DIR / f"{table}.csv"
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh, lineterminator="\n")
            writer.writerow(HEADER)
            writer.writerows(rows)
        print(f"{table}: {len(rows)} columns -> {path.name}")
    if missing:
        raise SystemExit(f"missing translations: {missing}")


if __name__ == "__main__":
    main()
