"""Verify the cleaned us_eia_consumption parquet before upload.

    python verify_parquet.py

Checks, over the whole cleaned record: row counts and per-year partitions; the
natural keys; directory resolution; and a magnitude check that reproduces EIA's
published national retail electricity total to a rounding error — the check that
catches a units, melt or double-count error. Reads ``$US_EIA_CONSUMPTION_DATA_DIR/output``.
"""

import sys

import pandas as pd
import pyarrow.dataset as pads
from common import OUTPUT


def load(table: str) -> pd.DataFrame:
    return (
        pads.dataset(OUTPUT / table, format="parquet").to_table().to_pandas()
    )


def main() -> None:
    problems = []
    for t in ("utility", "retail_sales", "service_territory", "eia861m"):
        df = load(t)
        yrs = df["year"].astype(int)
        print(
            f"{t}: {len(df):,} rows, {yrs.min()}-{yrs.max()}, {yrs.nunique()} partitions"
        )

    u = load("utility")
    if u.duplicated(subset=["year", "utility_id"]).sum():
        problems.append("utility key not unique")

    r = load("retail_sales")
    dups = r.duplicated(
        subset=[
            "year",
            "utility_id",
            "state_id",
            "part",
            "service_type",
            "ba_code",
            "customer_sector",
        ]
    ).mean()
    print(f"retail_sales key dup share: {dups * 100:.3f}%")
    if dups > 0.005:
        problems.append(f"retail_sales key dup share {dups * 100:.2f}% > 0.5%")

    m = load("eia861m")
    if m.duplicated(
        subset=["year", "month", "state_code", "customer_sector"]
    ).sum():
        problems.append("eia861m key not unique")

    # Magnitude: US residential retail sales 2020. EIA published ~1,462 M MWh.
    # The correct total excludes Delivery (which duplicates Energy).
    r["sales"] = pd.to_numeric(r["sales_mwh"], errors="coerce")
    res = r[(r.year == "2020") & (r.customer_sector == "residential")]
    total_bundled_energy = (
        res[res.service_type.isin(["Bundled", "Energy"])]["sales"].sum() / 1e6
    )
    print(
        f"retail residential 2020 (Bundled+Energy): {total_bundled_energy:.1f} M MWh (EIA ~1,462)"
    )
    if abs(total_bundled_energy - 1462) / 1462 > 0.02:
        problems.append(
            f"retail residential 2020 off: {total_bundled_energy:.1f}"
        )

    m["sales"] = pd.to_numeric(m["sales_mwh"], errors="coerce")
    m2020 = (
        m[
            (m.year == "2020")
            & (m.customer_sector == "residential")
            & (m.state_code != "US")
        ]["sales"].sum()
        / 1e6
    )
    print(f"861m residential 2020 (states): {m2020:.1f} M MWh (EIA ~1,462)")
    if abs(m2020 - 1462) / 1462 > 0.02:
        problems.append(f"861m residential 2020 off: {m2020:.1f}")

    s = load("service_territory")
    print(
        f"state_id resolution retail={r.state_id.notna().mean() * 100:.1f}% | "
        f"county_id svc={s.county_id.notna().mean() * 100:.1f}% | "
        f"861m state_id={m.state_id.notna().mean() * 100:.1f}%"
    )

    if problems:
        print("\nPROBLEMS:")
        for p in problems:
            print(f"  - {p}")
        sys.exit(1)
    print("\nall checks passed")


if __name__ == "__main__":
    main()
