"""Clean the NCSES HERD public use data files into Data Basis tables.

The transform itself lives in ``pipelines/datasets/us_nsf_ncses/utils.py`` so
the recurring flow and this one-shot bootstrap cannot drift apart; this script
only points it at the scratch directory and prints what it produced.

Source
------
NCSES publishes institution-level public use microdata for the Higher Education
Research and Development (HERD) Survey and for its predecessor, the Survey of
R&D Expenditures at Universities and Colleges, at
https://ncses.nsf.gov/explore-data/microdata/higher-education-research-development

Output
------
Five tables, hive-partitioned by ``year``, written all-STRING (the dbt models
``safe_cast`` each column to its architecture type):

* ``herd_institution``   one row per institution x fiscal year
* ``herd_expenditure``   every monetary questionnaire item, in USD
* ``herd_personnel``     headcount and full-time-equivalent R&D personnel
* ``herd_survey_item``   non-monetary items and capitalisation thresholds
* ``dicionario``         value -> label for every coded column

Run
---
    python models/us_nsf_ncses/code/herd_clean.py

Set ``NCSES_DATA_DIR`` to override the scratch location (default
``~/Downloads/us_nsf_ncses_data``); ``input/`` holds the downloaded ZIPs and
``output/`` receives the parquet.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from pipelines.datasets.us_nsf_ncses.utils import clean_herd  # noqa: E402

DATA_DIR = Path(
    os.environ.get(
        "NCSES_DATA_DIR", os.path.expanduser("~/Downloads/us_nsf_ncses_data")
    )
)


def main() -> int:
    """Clean every downloaded HERD file into the scratch output directory."""
    produced = clean_herd(DATA_DIR / "input", DATA_DIR / "output")
    print("\n== output ==")
    for table, path in produced.items():
        print(f"  {table:<20} {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
