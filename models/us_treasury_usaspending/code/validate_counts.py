"""Validate the cleaned row counts against USAspending's own transaction counts.

The bulk archive and the search API are independent surfaces over the same
records, so the API's per-fiscal-year transaction counts are a genuine external
check on the extract — not a restatement of it. The archive's award families
map onto the API's categories as:

    contract_transaction   = contracts + idvs
    assistance_transaction = grants + loans + direct_payments + other

Small differences are expected and not failures: the archive is a monthly
snapshot while the API is live, so a correction filed in between moves a count
by a handful of rows. Anything beyond a fraction of a percent is worth reading
as a real discrepancy.

The API only serves action dates from 2007-10-01, so FY2007 cannot be checked
this way.

Usage:
    uv run python models/us_treasury_usaspending/code/validate_counts.py
    uv run python models/us_treasury_usaspending/code/validate_counts.py --years 2010,2020
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date
from pathlib import Path

import requests

API = (
    "https://api.usaspending.gov/api/v2/search/spending_by_transaction_count/"
)
FIRST_API_FISCAL_YEAR = 2008
CONTRACT_KEYS = ("contracts", "idvs")
ASSISTANCE_KEYS = ("grants", "loans", "direct_payments", "other")
TOLERANCE = 0.001  # 0.1%
# The open fiscal year is measured against a snapshot that is by construction
# behind the live API — the archive is rebuilt monthly, so on any given day it
# is missing everything filed since the last build. That gap is the reason the
# recurring pipeline re-pulls the current fiscal year, not a defect, so it is
# reported as "open" rather than counted against the tolerance.
OPEN_YEAR_LABEL = "open"

DATA_DIR = Path(
    os.environ.get(
        "USASPENDING_DATA_DIR",
        Path.home() / "Downloads" / "us_treasury_usaspending_data",
    )
)


def api_counts(fiscal_year: int) -> dict[str, int]:
    body = {
        "filters": {
            "time_period": [
                {
                    "start_date": f"{fiscal_year - 1}-10-01",
                    "end_date": f"{fiscal_year}-09-30",
                    "date_type": "action_date",
                }
            ]
        }
    }
    resp = requests.post(API, json=body, timeout=600)
    resp.raise_for_status()
    return resp.json()["results"]


def current_fiscal_year(today: date | None = None) -> int:
    """US federal fiscal year of `today` — FY N runs Oct 1 N-1 to Sep 30 N."""
    today = today or date.today()
    return today.year + 1 if today.month >= 10 else today.year


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--years", help="e.g. 2010,2020 (default: every year present)"
    )
    ap.add_argument("--counts", default=str(DATA_DIR / "row_counts.json"))
    args = ap.parse_args()

    cleaned = json.loads(Path(args.counts).read_text())
    years = (
        [int(y) for y in args.years.split(",")]
        if args.years
        else sorted(
            {
                int(k.split("/")[1])
                for k in cleaned
                if "/" in k and int(k.split("/")[1]) >= FIRST_API_FISCAL_YEAR
            }
        )
    )

    print(
        f"{'FY':<6}{'table':<24}{'cleaned':>14}{'api':>14}{'diff':>10}  status"
    )
    open_fy = current_fiscal_year()
    worst = 0.0
    worst_closed = 0.0
    for fy in years:
        results = api_counts(fy)
        for table, keys in (
            ("contract_transaction", CONTRACT_KEYS),
            ("assistance_transaction", ASSISTANCE_KEYS),
        ):
            have = cleaned.get(f"{table}/{fy}")
            if have is None:
                continue
            want = sum(results.get(k, 0) for k in keys)
            diff = have - want
            rel = abs(diff) / want if want else 0.0
            if fy != open_fy:
                worst = max(worst, rel)
            if fy == open_fy:
                status = OPEN_YEAR_LABEL
            elif rel <= TOLERANCE:
                status = "ok"
            else:
                status = "CHECK"
                worst_closed = max(worst_closed, rel)
            print(
                f"{fy:<6}{table:<24}{have:>14,}{want:>14,}{diff:>10,}  {status}"
            )
    print(
        f"\nlargest relative difference among closed fiscal years: "
        f"{worst:.4%} (tolerance {TOLERANCE:.1%})"
    )
    print(
        f"FY{open_fy} is still open; its shortfall against the live API is the "
        "archive snapshot's age and is refreshed by the recurring pipeline."
    )
    if worst_closed:
        raise SystemExit("a closed fiscal year exceeded the tolerance")


if __name__ == "__main__":
    main()
