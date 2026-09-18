"""One-shot onboarding transform for au_tas_tec_elections.

Thin wrapper: every function lives in ``pipelines/datasets/au_tas_tec_elections`` so
a later recurring pipeline reuses this transform instead of duplicating it.

Run: PYTHONPATH=. ~/.venvs/bd-pipelines-tas/bin/python \
        models/au_tas_tec_elections/code/clean.py
"""

from __future__ import annotations

import json
import warnings

from pipelines.datasets.au_tas_tec_elections.constants import (
    constants,
    data_root,
)
from pipelines.datasets.au_tas_tec_elections.utils import clean_all


def main() -> int:
    warnings.filterwarnings("ignore")
    root = data_root()
    counts = clean_all(root / "input", root / "output")
    print(f"\n{'table':<34}{'rows':>12}")
    for table in constants.TABLES.value:
        print(f"{table:<34}{counts[table]:>12,}")
    (root / "output" / "row_counts.json").write_text(
        json.dumps(counts, indent=2), encoding="utf-8"
    )
    print(f"\ntotal {sum(counts.values()):,} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
