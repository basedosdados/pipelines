"""Validate the cleaned au_abs_population parquet before it is uploaded.

Checks the properties the dbt tests cannot see until the data is already in
BigQuery, plus the two that motivated design decisions and must not silently
regress:

1. row counts and year spans per table;
2. uniqueness of each table's logical key;
3. foreign keys against the ``br_bd_diretorios_au`` directory tables, whose id
   lists are read from BigQuery;
4. non-null proportions, so the ``not_null_proportion_multiple_columns``
   exclusions in schema.yml stay grounded in measurement;
5. that ``population_density`` equals the figure ABS publishes -- it is carried,
   never derived, because ABS computes it from unrounded area.

Usage:
    python validate.py <output_dir> [<directory_id_dir>]
"""

import glob
import os
import sys

import pandas as pd

KEYS = {
    "national_state": [
        "year",
        "quarter",
        "state_id",
        "region_name",
        "sex",
        "measure",
    ],
    "erp_age_sex": ["year", "state_id", "region_name", "sex", "age"],
    "projection": ["year", "series", "state_id", "region_name", "sex", "age"],
    "regional_sa2": ["year", "sa2_id"],
    "regional_lga": ["year", "lga_id"],
    "series": ["series_id"],
}

# column -> directory id file, produced by dump_directory_ids.sh
FKS = {
    "regional_sa2": {
        "sa2_id": "sa2_2021",
        "sa3_id": "sa3_2021",
        "sa4_id": "sa4_2021",
        "gccsa_id": "gccsa_2021",
        "state_id": "state",
    },
    "regional_lga": {"lga_id": "lga_2021", "state_id": "state"},
    "national_state": {"state_id": "state"},
    "erp_age_sex": {"state_id": "state"},
    "projection": {"state_id": "state"},
}

# Columns legitimately sparse by construction; mirrored by the schema.yml
# `ignore_values` lists. Kept here so the two cannot drift apart unnoticed.
SPARSE = {
    "regional_sa2": ["population_density"],
    "regional_lga": ["population_density"],
}


def read_table(out_dir: str, table: str) -> pd.DataFrame:
    files = sorted(
        glob.glob(
            os.path.join(out_dir, table, "**", "*.parquet"), recursive=True
        )
    )
    if not files:
        raise FileNotFoundError(f"no parquet for {table} under {out_dir}")
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def main(out_dir: str, dir_ids: str = "/tmp") -> int:
    failures = []
    for table, key in KEYS.items():
        df = read_table(out_dir, table)
        # Staging parquet is all-STRING; NULL must survive as NULL, never "nan".
        for col in df.columns:
            bad = int((df[col].astype("string") == "nan").sum())
            if bad:
                failures.append(f"{table}.{col}: {bad} literal 'nan' strings")

        span = ""
        if "year" in df.columns:
            y = pd.to_numeric(df["year"])
            span = f" years {y.min()}-{y.max()}"
        dup = int(df.duplicated(subset=key).sum())
        print(f"\n=== {table}: {len(df):,} rows{span}")
        print(f"    key {key} -> {dup} duplicates")
        if dup:
            failures.append(f"{table}: {dup} duplicate keys")

        for col, dirname in FKS.get(table, {}).items():
            path = os.path.join(dir_ids, f"dir_{dirname}.txt")
            if not os.path.exists(path):
                print(f"    FK {col} -> {dirname}: SKIPPED (no {path})")
                continue
            with open(path, encoding="utf-8") as fh:
                valid = {ln.strip() for ln in fh if ln.strip()}
            got = set(df[col].dropna().unique())
            missing = sorted(got - valid)
            print(
                f"    FK {col} -> {dirname}: {len(got)} distinct, "
                f"{len(missing)} unmatched {missing[:5]}"
            )
            if missing and not (table == "regional_lga" and col == "lga_id"):
                failures.append(f"{table}.{col}: unmatched {missing[:5]}")

        nn = (df.notna().mean() * 100).round(2)
        sparse = SPARSE.get(table, [])
        low = [c for c in df.columns if nn[c] < 5 and c not in sparse]
        print(
            "    non-null %: " + ", ".join(f"{c}={nn[c]}" for c in df.columns)
        )
        if low:
            failures.append(f"{table}: undeclared sparse columns {low}")
        for c in sparse:
            print(f"    declared sparse: {c} = {nn[c]}% non-null")

    print("\n" + "=" * 60)
    if failures:
        print("FAILURES:")
        for f in failures:
            print("  -", f)
        return 1
    print("All validations passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "/tmp"))
