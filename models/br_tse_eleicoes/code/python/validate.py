"""
Validation: compare Python outputs against Stata reference (.dta files).

Usage:
    python validate.py                          # validate all tables
    python validate.py candidatos               # validate one table
    python validate.py candidatos 2020          # validate one table x year
"""

import gc
import sys
import time

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


def _classify_columns(df_ref, df_test, shared):
    """Classify shared columns as float, numeric (int-like), or string.
    Returns (float_cols, numeric_cols, string_cols)."""
    float_cols = []
    numeric_cols = []
    string_cols = []
    for col in shared:
        ref_dtype = df_ref[col].dtype
        test_dtype = df_test[col].dtype
        if pd.api.types.is_float_dtype(
            ref_dtype
        ) or pd.api.types.is_float_dtype(test_dtype):
            float_cols.append(col)
        elif pd.api.types.is_integer_dtype(
            ref_dtype
        ) or pd.api.types.is_integer_dtype(test_dtype):
            numeric_cols.append(col)
        else:
            string_cols.append(col)
    return float_cols, numeric_cols, string_cols


def _normalize_col_to_str(series, is_float=False):
    """Convert a single column to normalized string, vectorized."""
    s = (
        series.fillna("")
        .astype(str)
        .replace({"nan": "", "None": "", "<NA>": ""})
    )
    # Strip trailing .0 from integer-like floats
    s = s.str.replace(r"\.0$", "", regex=True)
    if is_float:
        # Vectorized float32 normalization — no .apply(lambda)
        # Apply to ALL numeric values (not just those with decimal points), so that
        # Stata's "20000000.00" and Python's 20000000.0 both normalize to "2e+07".
        num = pd.to_numeric(s, errors="coerce")
        has_num = num.notna() & (s != "")
        if has_num.any():
            f32 = num[has_num].astype("float32").astype("float64")
            vals = f32.to_numpy()
            formatted = np.where(vals != 0, np.char.mod("%.7g", vals), "0")
            s = s.copy()
            s.loc[has_num] = formatted
    return s


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
        shared = [c for c in ref_cols if c in test_cols]
    else:
        shared = ref_cols

    if not shared or len(df_ref) == 0 or len(df_test) == 0:
        return issues

    # Classify columns once — only apply float normalization where needed
    float_cols, numeric_cols, string_cols = _classify_columns(
        df_ref, df_test, shared
    )
    print(
        f"({len(float_cols)}f/{len(numeric_cols)}i/{len(string_cols)}s cols) ",
        end="",
        flush=True,
    )

    # Sort original DataFrames by shared columns, then sample.
    # Only convert the SAMPLED rows to strings — avoids OOM on huge tables.
    print("sorting ...", end="", flush=True)
    ref_sorted = df_ref[shared].sort_values(shared).reset_index(drop=True)
    test_sorted = df_test[shared].sort_values(shared).reset_index(drop=True)

    n = min(len(ref_sorted), len(test_sorted))
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

    # Extract samples, then free the full sorted DataFrames
    ref_sample_raw = ref_sorted.iloc[sample_indices].reset_index(drop=True)
    test_sample_raw = test_sorted.iloc[sample_indices].reset_index(drop=True)
    del ref_sorted, test_sorted

    # Now normalize only the small sample to strings
    print(" comparing ...", end="", flush=True)
    float_set = set(float_cols)
    ref_parts = {}
    test_parts = {}
    for col in shared:
        is_float = col in float_set
        ref_parts[col] = _normalize_col_to_str(
            ref_sample_raw[col], is_float=is_float
        )
        test_parts[col] = _normalize_col_to_str(
            test_sample_raw[col], is_float=is_float
        )
    ref_sample = pd.DataFrame(ref_parts)
    test_sample = pd.DataFrame(test_parts)
    del ref_sample_raw, test_sample_raw

    for col in shared:
        mismatches = (ref_sample[col] != test_sample[col]).sum()
        if mismatches > 0:
            idx = (ref_sample[col] != test_sample[col]).idxmax()
            issues.append(
                f"[{label}] Column '{col}': {mismatches}/{len(ref_sample)} sampled rows differ. "
                f"First: ref='{ref_sample[col].iloc[idx]}' vs test='{test_sample[col].iloc[idx]}'"
            )

    # 4. Numeric column stats — only on actual numeric columns (reuse dtype classification)
    for col in float_cols + numeric_cols:
        ref_num = pd.to_numeric(df_ref[col], errors="coerce")
        test_num = pd.to_numeric(df_test[col], errors="coerce")
        ref_nn = ref_num.notna().sum()
        test_nn = test_num.notna().sum()
        if ref_nn > 0 and test_nn > 0:
            if ref_nn != test_nn:
                issues.append(
                    f"[{label}] Column '{col}' numeric count: ref={ref_nn}, test={test_nn}"
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

    total = len(years)
    for i, yr in enumerate(years, 1):
        label = f"{table}_{yr}"
        t0 = time.time()
        print(
            f"  [{i}/{total}] {label}: loading Stata .dta ...",
            end="",
            flush=True,
        )
        df_ref = load_stata_dta(table, yr)
        if df_ref is None:
            print(" NOT FOUND")
            all_issues.append(f"[{label}] Stata reference .dta not found")
            continue
        print(
            f" {len(df_ref)} rows. Loading Python parquet ...",
            end="",
            flush=True,
        )
        df_test = load_python_parquet(table, yr)
        if df_test is None:
            print(" NOT FOUND")
            all_issues.append(f"[{label}] Python parquet not found")
            del df_ref
            gc.collect()
            continue
        print(f" {len(df_test)} rows. Comparing ...", end="", flush=True)

        issues = compare_dataframes(df_ref, df_test, label)
        all_issues.extend(issues)
        elapsed = time.time() - t0

        if not issues:
            print(f" ✓ OK ({len(df_ref.columns)} cols) [{elapsed:.1f}s]")
        else:
            print(f" ✗ {len(issues)} issue(s) [{elapsed:.1f}s]")
            for iss in issues:
                print(f"      {iss}")

        del df_ref, df_test
        gc.collect()

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
