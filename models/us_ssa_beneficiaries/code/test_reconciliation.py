"""Fault-injection tests for the us_ssa_beneficiaries reconciliation gate.

The gate is the thing standing between a mis-parsed SSA edition and BigQuery,
so it is worth proving rather than asserting. Each test corrupts the cleaned
tables the way a real parse failure would and checks the gate raises.

Run with the cleaned tables already built:

    uv run python models/us_ssa_beneficiaries/code/test_reconciliation.py

It rebuilds them from ``~/Downloads/us_ssa_beneficiaries_data/input`` if needed
(override with ``SSA_DATA_DIR``).
"""

# ruff: noqa: E402  (sys.path must be set before the pipelines import)
from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from pipelines.datasets.us_ssa_beneficiaries import utils as U  # noqa: N812

DATA_DIR = Path(
    os.environ.get(
        "SSA_DATA_DIR", Path.home() / "Downloads" / "us_ssa_beneficiaries_data"
    )
)
YEAR = 2024
COUNT = "beneficiary_count"


def _expect_raise(
    label: str, county: pd.DataFrame, state: pd.DataFrame, expected: set[str]
) -> bool:
    try:
        U.reconcile_year(
            county, state, YEAR, COUNT, expected_county_areas=expected
        )
    except U.ReconciliationError as exc:
        print(f"  caught  {label}\n            {str(exc)[:110]}")
        return True
    print(f"  MISSED  {label}")
    return False


def main() -> int:
    input_dir = DATA_DIR / "input"
    county = U.build_oasdi_county(input_dir)
    state = U.build_oasdi_state(input_dir)
    expected = U.expected_county_areas(county, COUNT)

    print("baseline:")
    report = U.reconcile_year(
        county, state, YEAR, COUNT, expected_county_areas=expected
    )
    print(
        f"  passes, worst state gap {report['worst_state_gap']:+.4%}, "
        f"national {report['national_gap']:.5%}"
    )

    print("injected faults:")
    results = []

    # A bad join that duplicates rows. Suppression can only make a county sum
    # too low, so any excess is a fault.
    dup = county[(county.year == YEAR) & (county.state_name == "Texas")]
    results.append(
        _expect_raise(
            "county rows duplicated for one state",
            pd.concat([county, dup], ignore_index=True),
            state,
            expected,
        )
    )

    # A whole state's block dropped -- invisible to a per-state gap check,
    # because a vanished block leaves nothing to compare against.
    results.append(
        _expect_raise(
            "one state's county block dropped",
            county[
                ~((county.year == YEAR) & (county.state_name == "California"))
            ],
            state,
            expected,
        )
    )

    # A decimal misread in the state table.
    scaled = state.copy()
    mask = (scaled.year == YEAR) & (scaled.state_or_area == U.NATIONAL_LABEL)
    scaled.loc[mask, COUNT] = scaled.loc[mask, COUNT] * 10
    results.append(
        _expect_raise(
            "national total misread by a factor of ten",
            county,
            scaled,
            expected,
        )
    )

    print(f"{sum(results)}/{len(results)} faults caught")
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
