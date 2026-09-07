"""Validate the cleaned parquet against NCHS published totals.

The check that matters for this dataset: excluding foreign residents
(``residence_status = 4``) and, for births, weighting by ``record_weight``
should reproduce the figures NCHS publishes, exactly. Both adjustments are easy
to omit and neither fails loudly - a missing weight silently understates
1972-1984 births by up to 46 percent.

    python models/us_nchs_vital_statistics/code/us_nchs_vital_statistics_validate.py
"""

import os
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

DATA = Path(
    os.environ.get(
        "NCHS_DATA_DIR",
        os.path.expanduser("~/Downloads/us_nchs_vital_statistics_data"),
    )
)

# NCHS published totals (residents of the 50 states and DC).
PUBLISHED = {
    ("birth", 1968): 3501564,
    ("birth", 1970): 3731386,
    ("birth", 1972): 3258411,
    ("birth", 1975): 3144198,
    ("birth", 1980): 3612258,
    ("birth", 1985): 3760561,
    ("birth", 1989): 4040958,
    ("birth", 2003): 4089950,
    ("birth", 2015): 3978497,
    ("birth", 2017): 3855500,
    ("birth", 2019): 3747540,
    ("birth", 2020): 3613647,
    ("birth", 2021): 3664292,
    ("birth", 2022): 3667758,
    ("death", 1968): 1930082,
    ("death", 1990): 2148463,
    ("death", 2000): 2403351,
    ("death", 2003): 2448288,
    ("death", 2004): 2397615,
    ("death", 2010): 2468435,
    ("death", 2019): 2854838,
    ("death", 2020): 3383729,
}


def measured(product: str, year: int) -> int | None:
    f = DATA / "output" / product / f"year={year}" / "data.parquet"
    if not f.exists():
        return None
    cols = ["residence_status"] + (
        ["record_weight"] if product == "birth" else []
    )
    df = pq.ParquetFile(f).read(columns=cols).to_pandas()
    resident = df["residence_status"].ne("4")
    if product == "birth":
        weight = pd.to_numeric(df["record_weight"], errors="coerce").fillna(1)
        return int(weight[resident].sum())
    return int(resident.sum())


def main():
    ok = bad = skipped = 0
    for (product, year), published in sorted(PUBLISHED.items()):
        got = measured(product, year)
        if got is None:
            print(f"  {product} {year}: not written, skipped")
            skipped += 1
            continue
        match = got == published
        ok, bad = (ok + 1, bad) if match else (ok, bad + 1)
        flag = "OK " if match else "MISMATCH"
        print(
            f"  {flag} {product} {year}: {got:>10,} vs published {published:>10,}"
            + ("" if match else f"  (diff {got - published:+,})")
        )

    # Coverage: every year the source publishes must be present.
    print()
    for product in ("birth", "death"):
        d = DATA / "output" / product
        years = sorted(
            int(p.name.split("=")[1])
            for p in d.glob("year=*")
            if (p / "data.parquet").exists()
        )
        # 1969 natality has no published record layout anywhere.
        expected = [
            y
            for y in range(1968, 2025)
            if not (product == "birth" and y == 1969)
        ]
        missing = [y for y in expected if y not in years]
        rows, unreadable = 0, []
        for y in years:
            try:
                rows += pq.ParquetFile(
                    d / f"year={y}" / "data.parquet"
                ).metadata.num_rows
            except Exception:  # a partition still being written has no footer
                unreadable.append(y)
        print(
            f"  {product}: {len(years)}/{len(expected)} years, {rows:,} rows"
        )
        if missing:
            print(f"    MISSING: {missing}")
        if unreadable:
            print(f"    UNREADABLE (mid-write or corrupt): {unreadable}")

    print(f"\n{ok} matched, {bad} mismatched, {skipped} skipped")
    if bad:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
