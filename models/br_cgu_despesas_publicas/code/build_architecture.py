"""Generate the architecture CSV for br_cgu_despesas_publicas.

Usage:
    uv run python models/br_cgu_despesas_publicas/code/build_architecture.py

Reads ``_spec.py`` — the single source of truth for the column list — and writes
``architecture/execucao.csv``. ``build_columns_json.py`` writes the trilingual
backend payload from the same spec, so the two artifacts cannot drift.

Note the explicit ``lineterminator="\\n"``: ``csv.writer`` defaults to ``\\r\\n``,
which the repo's mixed-line-ending pre-commit hook rewrites on every commit.
"""

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

# pyrefly: ignore [missing-import]
from _spec import SPEC

OUT = Path(__file__).resolve().parent / "architecture" / "execucao.csv"
PERIOD_COLUMN = "Ano e mês do lançamento"
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
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(HEADER)
        for name, typ, src, pt, _en, _es, unit, dc, obs in SPEC:
            w.writerow(
                [
                    name,
                    typ,
                    pt,
                    "",
                    "no",
                    dc,
                    unit,
                    "no",
                    obs,
                    src or PERIOD_COLUMN,
                ]
            )
    print(f"wrote {OUT} ({len(SPEC)} columns)")


if __name__ == "__main__":
    main()
