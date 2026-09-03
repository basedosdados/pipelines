"""Trim the NODC master concordance to the variables this dataset reads.

Source: https://github.com/Nonprofit-Open-Data-Collective/ef2
        (inst/extdata/concordance.csv, MIT licence), the maintained successor
        of the irs-efile-master-concordance-file repository.

The full file has ~6,900 rows spanning every part and schedule; the pipeline
ships only the rows for ``variables.ALL_VARIABLES`` so the worker never fetches
GitHub at run time. Re-run when the upstream concordance gains a schema
version::

    python build_concordance.py --source /path/to/concordance.csv
"""

import argparse
import csv
from pathlib import Path

from pipelines.datasets.us_irs_form990.constants import constants
from pipelines.datasets.us_irs_form990.variables import ALL_VARIABLES

KEEP = ["variable_name", "xpath", "form_type", "rdb_table", "data_type_simple"]


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", required=True, type=Path)
    args = p.parse_args()
    want = set(ALL_VARIABLES)
    # The upstream CSV is not valid UTF-8 (a few Windows-1252 quotes).
    with open(args.source, newline="", encoding="latin-1") as fh:
        rows = [r for r in csv.DictReader(fh) if r["variable_name"] in want]
    found = {r["variable_name"] for r in rows}
    missing = want - found
    if missing:
        raise SystemExit(f"variables absent from the concordance: {missing}")
    rows.sort(key=lambda r: (r["variable_name"], r["form_type"], r["xpath"]))
    dest = constants.CONCORDANCE_PATH.value
    with open(dest, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=KEEP, lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({k: r[k] for k in KEEP})
    print(f"{dest}: {len(rows)} rows, {len(found)} variables")


if __name__ == "__main__":
    main()
