#!/usr/bin/env python3
"""Build the `dictionary` table for us_census_cps.

One row per (table, column, code, temporal coverage) -> label, for every column
flagged `covered_by_dictionary = yes` in the architecture.

Labels come from three sources, in priority order:

1. **CEPR value labels** attached to the built `.dta` files. These are the
   authoritative labels the harmonisation itself assigns, read straight off the
   Stata files rather than re-parsed out of the `label define` blocks, so they
   are exactly the sets attached to the columns we ship. Label sets change
   across years (industry/occupation schemes especially) -- each distinct
   code->label mapping gets its own `cobertura_temporal`.
2. **Binary flags.** CEPR leaves its 0/1 indicators unlabelled; every such
   column is verified to take no value outside {0, 1} before being written out
   as 0 = No / 1 = Yes.
3. **Month in sample** (`minsamp`, `mis`), whose codes are self-describing.

A column flagged for the dictionary that none of the three can label is a hard
error -- it means the flag is wrong, not that the row should be skipped.

Usage:
    python3 build_dictionary.py --labels labels_scan.json \
        --nonnull nonnull_years.json --out <parquet_root>/dictionary
"""

import argparse
import csv
import glob
import json
import os
import re
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq

HERE = os.path.dirname(os.path.abspath(__file__))
ARCH = os.path.join(HERE, "architecture")
TABLES = {
    "org": "org.csv",
    "basic_monthly": "basic_monthly.csv",
    "march": "march.csv",
}

BINARY = {"0": "No", "1": "Yes"}
MIS_COLS = {"minsamp", "mis"}


def ranges(years):
    ys = sorted(int(y) for y in years)
    out, start, prev = [], ys[0], ys[0]
    for y in ys[1:]:
        if y == prev + 1:
            prev = y
            continue
        out.append((start, prev))
        start = prev = y
    out.append((start, prev))
    return ", ".join(f"{a}(1){b}" for a, b in out)


def distinct_values(parquet_root, table, cols):
    """Distinct non-null values per column, straight from the fixture."""
    acc = {c: set() for c in cols}
    for f in sorted(
        glob.glob(f"{parquet_root}/{table}/**/data.parquet", recursive=True)
    ):
        tb = pq.read_table(f, columns=list(cols))
        for c in cols:
            acc[c] |= {
                v for v in tb.column(c).unique().to_pylist() if v is not None
            }
    return acc


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--labels", required=True)
    ap.add_argument("--nonnull", required=True)
    ap.add_argument(
        "--parquet", default=os.path.expanduser("~/cps_build/parquet")
    )
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    with open(args.labels) as fh:
        labels = json.load(fh)
    with open(args.nonnull) as fh:
        nonnull = json.load(fh)

    rows = []
    for table, fname in TABLES.items():
        with open(os.path.join(ARCH, fname)) as fh:
            arch = list(csv.DictReader(fh))
        dict_cols = [
            r["name"]
            for r in arch
            if r["covered_by_dictionary"].strip().lower() == "yes"
        ]
        full_years = set()
        for d in nonnull[table].values():
            full_years |= set(d)

        labelled = labels[table]
        unlabelled = [c for c in dict_cols if c not in labelled]
        # sources 2 and 3 need the observed value set to be safe
        observed = (
            distinct_values(args.parquet, table, unlabelled)
            if unlabelled
            else {}
        )

        for col in dict_cols:
            # ---- source 1: CEPR value labels, grouped by identical mapping ----
            if col in labelled:
                by_pair = defaultdict(set)  # (code, label) -> years
                for year, mapping in labelled[col].items():
                    for code, label in mapping.items():
                        by_pair[(code, label)].add(year)
                for (code, label), years in by_pair.items():
                    rows.append(
                        (
                            table,
                            col,
                            code,
                            "" if years == full_years else ranges(years),
                            label,
                        )
                    )
                continue

            years = set(nonnull[table].get(col, {}))
            cov = (
                "" if years == full_years else (ranges(years) if years else "")
            )
            vals = observed.get(col, set())

            # ---- source 3: month in sample ----
            if col in MIS_COLS:
                if not vals or not vals <= {str(i) for i in range(1, 9)}:
                    raise ValueError(
                        f"{table}.{col}: month-in-sample codes must be a "
                        f"non-empty subset of 1-8, got {sorted(vals)}"
                    )
                for code in sorted(vals, key=int):
                    rows.append(
                        (table, col, code, cov, f"Month in sample {code}")
                    )
                continue

            # ---- source 2: binary flag ----
            if not vals or not vals <= set(BINARY):
                raise ValueError(
                    f"{table}.{col} is flagged covered_by_dictionary=yes but has no "
                    f"CEPR label set and takes non-binary or empty values "
                    f"{sorted(vals)[:12]}"
                )
            for code in sorted(vals):
                rows.append((table, col, code, cov, BINARY[code]))

        print(
            f"{table}: {len(dict_cols)} dictionary columns "
            f"({len(dict_cols) - len(unlabelled)} CEPR-labelled, {len(unlabelled)} derived)",
            flush=True,
        )

    # deterministic order: table, column, then numeric code where possible
    def sort_key(r):
        m = re.fullmatch(r"-?\d+", r[2])
        return (r[0], r[1], 0 if m else 1, int(r[2]) if m else 0, r[2], r[3])

    rows.sort(key=sort_key)

    schema = pa.schema(
        [
            (c, pa.string())
            for c in [
                "id_tabela",
                "nome_coluna",
                "chave",
                "cobertura_temporal",
                "valor",
            ]
        ]
    )
    os.makedirs(args.out, exist_ok=True)
    pq.write_table(
        pa.Table.from_pydict(
            {k: [r[i] for r in rows] for i, k in enumerate(schema.names)},
            schema=schema,
        ),
        os.path.join(args.out, "data.parquet"),
        compression="snappy",
    )
    print(f"\ndictionary: {len(rows):,} rows -> {args.out}/data.parquet")


if __name__ == "__main__":
    main()
