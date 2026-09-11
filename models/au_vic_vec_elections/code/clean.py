"""One-shot onboarding transform for au_vic_vec_elections.

Thin wrapper: every function lives in ``pipelines/datasets/au_vic_vec_elections`` so a
later recurring pipeline reuses this transform instead of duplicating it.

Run:
    PYTHONPATH=. ~/.venvs/bd-pipelines-vic/bin/python models/au_vic_vec_elections/code/clean.py
"""

from __future__ import annotations

import warnings

from pipelines.datasets.au_vic_vec_elections.constants import (
    constants,
    data_root,
)
from pipelines.datasets.au_vic_vec_elections.utils import clean_all

# Row counts measured against the source during reconnaissance. They are assertions,
# not documentation: each is the number a specific source trap changes if mishandled.
EXPECTED: dict[str, int] = {}


def main() -> int:
    # openpyxl and xlrd warn on almost every VEC workbook; the headers are
    # non-standard but the data reads correctly.
    warnings.filterwarnings("ignore")
    root = data_root()
    counts = clean_all(root / "input", root / "output")

    print(f"\n{'table':<34}{'rows':>12}{'expected':>12}  status")
    failures = []
    for table in constants.TABLES.value:
        rows = counts[table]
        expected = EXPECTED.get(table)
        if expected is None:
            status = "-"
        elif rows == expected:
            status = "ok"
        else:
            status = f"MISMATCH ({rows - expected:+d})"
            failures.append(table)
        print(f"{table:<34}{rows:>12,}{(expected or 0):>12,}  {status}")

    if failures:
        print(f"\nrow-count mismatches: {failures}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
