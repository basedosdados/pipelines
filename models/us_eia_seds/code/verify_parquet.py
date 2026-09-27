"""Verify the cleaned us_eia_seds parquet before upload.

    python verify_parquet.py

Checks, over the whole cleaned record:

* row count and per-year partition counts;
* the natural key (year, state_code, msn) is exactly unique;
* the key columns are non-null and every value carries a unit and a measure_type;
* value parses as a number, and a few known SEDS national totals reproduce
  EIA's published figures to a rounding error — the check that would catch a
  units or decode error.

Reads ``$US_EIA_SEDS_DATA_DIR/output``.
"""

import sys

import pandas as pd
import pyarrow.dataset as pads
from common import OUTPUT

# Known SEDS national totals (StateCode US), for a magnitude check. Values are
# read straight from the published Complete_SEDS.csv, so this guards the decode
# and the stringify round-trip, not EIA's arithmetic.
KNOWN = {
    # (msn, year): (approx value, unit fragment)
    ("TETCB", "2022"): (94_937_305, "Billion Btu"),  # total energy consumption
    ("TETCB", "2000"): (96_686_060, "Billion Btu"),
}


def load() -> pd.DataFrame:
    data = pads.dataset(OUTPUT / "seds_consumption", format="parquet")
    return data.to_table().to_pandas()


def main() -> None:
    df = load()
    problems = []
    print(f"rows: {len(df):,}")
    years = df["year"].astype(int)
    print(f"years: {years.min()}-{years.max()}, {years.nunique()} partitions")

    dups = df.duplicated(subset=["year", "state_code", "msn"]).sum()
    print(f"duplicate (year, state_code, msn): {dups}")
    if dups:
        problems.append(f"{dups} duplicate keys")

    for col in ("year", "state_code", "msn", "measure_type"):
        nulls = df[col].isna().sum()
        if nulls:
            problems.append(f"{col} has {nulls} nulls")
    unit_missing = (df["measurement_unit"].isna() & df["msn"].notna()).sum()
    print(f"rows with msn but no unit: {unit_missing}")
    if unit_missing:
        problems.append(f"{unit_missing} rows missing a unit")

    value = pd.to_numeric(df["value"], errors="coerce")
    bad_value = (value.isna() & df["value"].notna()).sum()
    print(f"unparseable non-null values: {bad_value}")
    if bad_value:
        problems.append(f"{bad_value} unparseable values")

    for (msn, year), (expected, unit) in KNOWN.items():
        row = df[(df.msn == msn) & (df.state_code == "US") & (df.year == year)]
        if row.empty:
            problems.append(f"{msn}/{year} missing")
            continue
        got = float(row.iloc[0]["value"])
        got_unit = row.iloc[0]["measurement_unit"]
        ok = abs(got - expected) / expected < 0.001 and unit in str(got_unit)
        print(
            f"{msn}/{year}: {got:,.0f} {got_unit} "
            f"(expected ~{expected:,} {unit}) {'OK' if ok else 'MISMATCH'}"
        )
        if not ok:
            problems.append(f"{msn}/{year} magnitude/unit off")

    if problems:
        print("\nPROBLEMS:")
        for p in problems:
            print(f"  - {p}")
        sys.exit(1)
    print("\nall checks passed")


if __name__ == "__main__":
    main()
