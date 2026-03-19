"""
Validation: compare Python outputs against Stata reference (.dta files).

Usage:
    python validate.py                          # validate all tables
    python validate.py candidatos               # validate one table
    python validate.py candidatos 2020          # validate one table x year
"""

import sys

import numpy as np
import pandas as pd
from config import OUTPUT_PYTHON, OUTPUT_STATA


def load_stata_dta(table: str, year: int) -> pd.DataFrame | None:
    """Load a Stata .dta reference file, e.g. output/candidatos_2020.dta."""
    path = OUTPUT_STATA / f"{table}_{year}.dta"
    if not path.exists():
        return None
    return pd.read_stata(path, convert_categoricals=False)


def load_python_parquet(table: str, year: int) -> pd.DataFrame | None:
    """Load a Python intermediate parquet, e.g. output_python/{table}_{year}.parquet."""
    path = OUTPUT_PYTHON / f"{table}_{year}.parquet"
    if not path.exists():
        return None
    return pd.read_parquet(path)


def compare_dataframes(
    df_ref: pd.DataFrame,
    df_test: pd.DataFrame,
    label: str,
) -> list[str]:
    """
    Compare two DataFrames and return a list of discrepancy messages.
    Empty list means they match.
    """
    issues = []

    # 1. Row count
    if len(df_ref) != len(df_test):
        issues.append(
            f"[{label}] Row count: ref={len(df_ref)}, test={len(df_test)}"
        )

    # 2. Column names and order
    ref_cols = list(df_ref.columns)
    test_cols = list(df_test.columns)
    if ref_cols != test_cols:
        missing = set(ref_cols) - set(test_cols)
        extra = set(test_cols) - set(ref_cols)
        msg = f"[{label}] Column mismatch."
        if missing:
            msg += f" Missing: {sorted(missing)}."
        if extra:
            msg += f" Extra: {sorted(extra)}."
        if ref_cols != test_cols and not missing and not extra:
            msg += " Order differs."
        issues.append(msg)
        # Can only compare shared columns
        shared = [c for c in ref_cols if c in test_cols]
    else:
        shared = ref_cols

    if not shared or len(df_ref) == 0 or len(df_test) == 0:
        return issues

    # 3. Value comparison (sample-based)
    n = min(len(df_ref), len(df_test))
    sample_indices = sorted(
        set(
            list(range(min(100, n)))
            + list(range(max(0, n - 100), n))
            + list(
                np.random.default_rng(42).choice(
                    n, size=min(100, n), replace=False
                )
            )
        )
    )

    ref_sample = df_ref.iloc[sample_indices][shared].reset_index(drop=True)
    test_sample = df_test.iloc[sample_indices][shared].reset_index(drop=True)

    # Cast everything to string for comparison (avoids type mismatch noise)
    ref_str = ref_sample.astype(str).replace(
        {"nan": "", "None": "", "<NA>": ""}
    )
    test_str = test_sample.astype(str).replace(
        {"nan": "", "None": "", "<NA>": ""}
    )

    for col in shared:
        mismatches = (ref_str[col] != test_str[col]).sum()
        if mismatches > 0:
            # Show first mismatch
            idx = (ref_str[col] != test_str[col]).idxmax()
            issues.append(
                f"[{label}] Column '{col}': {mismatches}/{len(ref_str)} sampled rows differ. "
                f"First: ref='{ref_str[col].iloc[idx]}' vs test='{test_str[col].iloc[idx]}'"
            )

    # 4. Numeric column stats
    for col in shared:
        ref_num = pd.to_numeric(df_ref[col], errors="coerce")
        test_num = pd.to_numeric(df_test[col], errors="coerce")
        if ref_num.notna().sum() > 0 and test_num.notna().sum() > 0:
            if ref_num.notna().sum() != test_num.notna().sum():
                issues.append(
                    f"[{label}] Column '{col}' numeric count: "
                    f"ref={ref_num.notna().sum()}, test={test_num.notna().sum()}"
                )
            ref_sum = ref_num.sum()
            test_sum = test_num.sum()
            if not np.isclose(ref_sum, test_sum, rtol=1e-6, equal_nan=True):
                issues.append(
                    f"[{label}] Column '{col}' sum: ref={ref_sum}, test={test_sum}"
                )

    return issues


def validate_table(table: str, year: int | None = None) -> list[str]:
    """Validate a table (optionally for a single year)."""
    all_issues = []

    if year is not None:
        years = [year]
    else:
        # Discover available years from Stata output
        years = sorted(
            int(p.stem.split("_")[-1])
            for p in OUTPUT_STATA.glob(f"{table}_*.dta")
            if p.stem.split("_")[-1].isdigit()
        )

    for yr in years:
        label = f"{table}_{yr}"
        df_ref = load_stata_dta(table, yr)
        df_test = load_python_parquet(table, yr)

        if df_ref is None:
            all_issues.append(f"[{label}] Stata reference .dta not found")
            continue
        if df_test is None:
            all_issues.append(f"[{label}] Python parquet not found")
            continue

        issues = compare_dataframes(df_ref, df_test, label)
        all_issues.extend(issues)

        if not issues:
            print(
                f"  ✓ {label}: OK ({len(df_ref)} rows, {len(df_ref.columns)} cols)"
            )
        else:
            print(f"  ✗ {label}: {len(issues)} issue(s)")

    return all_issues


def discover_tables() -> list[str]:
    """Discover table names from Stata .dta files."""
    tables = set()
    for p in OUTPUT_STATA.glob("*.dta"):
        # e.g. candidatos_2020.dta → candidatos
        parts = p.stem.rsplit("_", 1)
        if len(parts) == 2 and parts[1].isdigit():
            tables.add(parts[0])
    return sorted(tables)


def main():
    args = sys.argv[1:]

    if len(args) == 0:
        tables = discover_tables()
        if not tables:
            print(f"No .dta files found in {OUTPUT_STATA}")
            return
        print(f"Validating {len(tables)} table(s)...\n")
    elif len(args) == 1:
        tables = [args[0]]
    elif len(args) == 2:
        table, year = args[0], int(args[1])
        issues = validate_table(table, year)
        _print_summary(issues)
        return
    else:
        print("Usage: python validate.py [table] [year]")
        return

    all_issues = []
    for table in tables:
        print(f"--- {table} ---")
        issues = validate_table(table)
        all_issues.extend(issues)
        print()

    _print_summary(all_issues)


def _print_summary(issues: list[str]):
    if not issues:
        print("\nAll validations passed.")
    else:
        print(f"\n{len(issues)} issue(s) found:")
        for issue in issues:
            print(f"  - {issue}")


if __name__ == "__main__":
    main()
