"""One-shot onboarding transform for au_ecq_elections.

Thin wrapper: every function lives in ``pipelines/datasets/au_ecq_elections/utils.py``
so a later recurring pipeline reuses this transform instead of duplicating it.

Run:
    PYTHONPATH=. ~/.venvs/bd-pipelines/bin/python models/au_ecq_elections/code/clean.py
"""

from __future__ import annotations

import sys

from pipelines.datasets.au_ecq_elections.constants import constants, data_root
from pipelines.datasets.au_ecq_elections.utils import (
    clean_all,
    download_disclosures,
    download_results,
    extract_results,
)

# Row counts measured directly against the source during reconnaissance. They are
# assertions, not documentation: every one of them is the number a specific source trap
# changes if it is mishandled.
EXPECTED = {
    "election": 54,
    "candidate": 4_356,
    "enrolment_turnout": 2_820,
    "result_district": 9_584,
    "result_voting_centre": 191_023,
    "distribution_of_preferences": 4_899,
    "voting_centre": 11_302,
    "disclosure_gift": 28_328,
    "disclosure_expenditure": 27_225,
    "disclosure_return": 293,
    # Not measured against the source: the dictionary is declared in
    # utils.VOCABULARIES and its build already fails when a value observed in a
    # cleaned table has no label. Pinned so a silent vocabulary edit shows up.
    "dicionario": 100,
}


def main(download: bool = False) -> int:
    root = data_root()
    input_dir = root / "input"
    output_dir = root / "output"

    if download:
        download_results(input_dir)
        download_disclosures(input_dir)
    extract_results(input_dir)

    counts = clean_all(input_dir, output_dir)

    print(f"{'table':<32}{'rows':>12}{'expected':>12}  status")
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
        print(f"{table:<32}{rows:>12,}{(expected or 0):>12,}  {status}")

    if failures:
        print(f"\nrow-count mismatches: {failures}")
        return 1
    print("\nall measured row counts match")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(download="--download" in sys.argv))
