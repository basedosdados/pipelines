"""Clean the NCSES SED published data tables into Data Basis tables.

The transform itself lives in ``pipelines/datasets/us_nsf_ncses/utils.py`` so
the recurring flow and this one-shot bootstrap cannot drift apart; this script
only points it at the scratch directory and prints what it produced.

Source
------
The Survey of Earned Doctorates releases one publication per survey cycle
("Doctorate Recipients from U.S. Universities"; NSF 25-349 is the 2024 cycle),
each shipping ~96 numbered data tables as Excel workbooks in one ZIP.

SED *microdata* is restricted-use and is deliberately not touched here: only the
published aggregate tables are in scope.

Output
------
Two tables, hive-partitioned by ``reference_year`` (the survey cycle):

* ``sed_data_table``  one row per published table: id, title, unit statement
* ``sed_estimate``    one row per published cell, fully long

Each cycle restates its own history, so a cycle is a self-contained vintage:
filter to the most recent ``reference_year`` for the current numbers rather than
summing across cycles.

Run
---
    python models/us_nsf_ncses/code/sed_clean.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from pipelines.datasets.us_nsf_ncses.utils import clean_sed  # noqa: E402

DATA_DIR = Path(
    os.environ.get(
        "NCSES_DATA_DIR", os.path.expanduser("~/Downloads/us_nsf_ncses_data")
    )
)


def main() -> int:
    """Parse every downloaded SED cycle into the scratch output directory."""
    produced = clean_sed(
        DATA_DIR / "input", DATA_DIR / "output", DATA_DIR / "work" / "sedx"
    )
    print("\n== output ==")
    for table, path in produced.items():
        print(f"  {table:<20} {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
