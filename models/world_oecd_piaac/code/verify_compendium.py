"""Reproduce the OECD's own published figures from the loaded tables.

Usage:
    uv run python models/world_oecd_piaac/code/verify_compendium.py

The OECD publishes data compendia precisely so that users can confirm they are
reading the Public Use Files correctly, which makes this the strongest end-to-end
check available: it exercises the sampling weights, the row selection, the country
assignment, the null handling and the preservation of categorical codes at once.

One subtlety the compendium settles: OECD counts only true system-missing values
as "Missing". The SAS special-missing letters -- D (don't know), N (not stated),
R (refused) -- are *valid* responses and belong in the denominator, which is
exactly how they are stored here.
"""

from __future__ import annotations

import collections
import sys
from pathlib import Path

import openpyxl
from google.cloud import bigquery

sys.path.insert(0, str(Path(__file__).parent))

import constants as piaac

COUNTRIES = {
    "Austria": "AUT",
    "Japan": "JPN",
    "Italy": "ITA",
    "France": "FRA",
    "Poland": "POL",
}
VARIABLE = "B_Q01a"
TOLERANCE = 1e-4


def published() -> dict[str, dict]:
    path = piaac.DOCS_ROOT / "cycle_1" / "compendium_background_round_1.xlsx"
    sheet = openpyxl.load_workbook(path, read_only=True, data_only=True)[
        VARIABLE
    ]
    rows = [
        r for r in sheet.iter_rows(min_row=1, max_row=40, values_only=True)
    ]
    first_category = next(j for j in range(4, len(rows[1]), 2) if rows[1][j])
    out = {}
    for row in rows[3:]:
        name = str(row[0]).strip() if row[0] else ""
        if name in COUNTRIES:
            out[COUNTRIES[name]] = {
                "weighted_n_all": float(row[1]),
                "missing_pct": float(row[2]),
                "weighted_n_valid": float(row[3]),
                "first_category_pct": float(row[first_category]),
            }
    return out


def observed() -> dict[str, dict[str, float]]:
    isos = "', '".join(COUNTRIES.values())
    query = f"""
        select country_id_iso_3 as iso, {VARIABLE.lower()} as val, sum(spfwt0) as w
        from `basedosdados-dev.world_oecd_piaac.respondent_cycle_1`
        where country_id_iso_3 in ('{isos}') and year = 2012
        group by 1, 2
    """
    by_country: dict[str, dict[str, float]] = collections.defaultdict(dict)
    for row in (
        bigquery.Client(project="basedosdados-dev").query(query).result()
    ):
        by_country[row.iso][row.val] = row.w
    return by_country


def main() -> None:
    expected, actual = published(), observed()
    failures = 0
    header = f"{'':6}{'weighted N (all)':>20}{'valid':>18}{'missing %':>12}{'category 1 %':>14}"
    print(header)
    for iso in COUNTRIES.values():
        weights = actual[iso]
        total = sum(weights.values())
        valid = sum(w for value, w in weights.items() if value is not None)
        missing_pct = 100 * (total - valid) / total
        category_pct = 100 * weights.get("1", 0.0) / valid
        want = expected[iso]

        checks = [
            (total, want["weighted_n_all"]),
            (valid, want["weighted_n_valid"]),
            (missing_pct, want["missing_pct"]),
            (category_pct, want["first_category_pct"]),
        ]
        ok = all(
            abs(a - b) <= max(TOLERANCE, abs(b) * 1e-6) for a, b in checks
        )
        failures += not ok
        print(
            f"{iso:6}{total:>20,.2f}{valid:>18,.2f}{missing_pct:>12.5f}"
            f"{category_pct:>14.5f}   {'OK' if ok else 'MISMATCH'}"
        )
        if not ok:
            print(f"       OECD: {want}")

    print(
        f"\n{len(COUNTRIES) - failures}/{len(COUNTRIES)} countries reproduce the OECD compendium exactly"
    )
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
