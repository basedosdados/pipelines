"""Cleaning code for Data Basis dataset in_tcpd_elections.

TCPD Lok Dhaba India elections (general / Lok Sabha and assembly / Vidhan Sabha).

Schema is driven entirely by the architecture CSVs under ``architecture/``:
the rename map (original_name -> name), the type map (name -> bigquery_type),
and the final column order are all derived by reading those files. Nothing about
the target schema is hardcoded here.

Reference tables under ``data/`` (state crosswalk, state directory, dictionary)
are prepared upstream and only converted to parquet as-is.

Outputs (Snappy parquet, hive-partitioned by ``year``):
    output/general_elections/year=<YYYY>/data.parquet
    output/assembly_elections/year=<YYYY>/data.parquet
    output/dictionary/data.parquet          (single file)
    output/state/data.parquet               (single file)
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
import reference as ref

# --------------------------------------------------------------------------- #
# Paths
# --------------------------------------------------------------------------- #
ROOT = Path(__file__).resolve().parents[1]
INPUT_DIR = ROOT / "input"
ARCH_DIR = ROOT / "architecture"
OUTPUT_DIR = ROOT / "output"

RAW_FILES = {
    "general_elections": INPUT_DIR / "All_States_GE.csv.gz",
    "assembly_elections": INPUT_DIR / "All_States_AE.csv.gz",
}
ARCH_FILES = {
    "general_elections": ARCH_DIR / "general_elections.csv",
    "assembly_elections": ARCH_DIR / "assembly_elections.csv",
}
EXPECTED_ROWS = {"general_elections": 91669, "assembly_elections": 483565}

# bigquery_type -> pyarrow type
PA_TYPE = {
    "INT64": pa.int64(),
    "FLOAT64": pa.float64(),
    "STRING": pa.string(),
    "BOOL": pa.bool_(),
}


# --------------------------------------------------------------------------- #
# Architecture
# --------------------------------------------------------------------------- #
def load_architecture(path: Path) -> pd.DataFrame:
    """Return the architecture rows with name, bigquery_type, original_name."""
    arch = pd.read_csv(path, dtype=str, keep_default_na=False)
    arch = arch[["name", "bigquery_type", "original_name"]].copy()
    arch["name"] = arch["name"].str.strip()
    arch["bigquery_type"] = arch["bigquery_type"].str.strip().str.upper()
    arch["original_name"] = arch["original_name"].str.strip()
    arch = arch[arch["name"] != ""].reset_index(drop=True)
    return arch


# --------------------------------------------------------------------------- #
# Safe casts
# --------------------------------------------------------------------------- #
def cast_string(s: pd.Series) -> pd.Series:
    """Strip; empty string -> null. Nullable pandas string dtype."""
    out = s.astype("string").str.strip()
    out = out.mask(out == "", pd.NA)
    return out


def cast_int64(s: pd.Series) -> pd.Series:
    """Coercing cast to nullable Int64 (bad values -> null)."""
    num = pd.to_numeric(s.replace("", np.nan), errors="coerce")
    return num.astype("Int64")


def cast_float64(s: pd.Series) -> pd.Series:
    """Coercing cast to float (bad values -> null)."""
    return pd.to_numeric(s.replace("", np.nan), errors="coerce")


_BOOL_MAP = {
    "TRUE": True,
    "FALSE": False,
    "YES": True,
    "NO": False,
    "T": True,
    "F": False,
    "1": True,
    "0": False,
}


def cast_bool(s: pd.Series) -> pd.Series:
    """Map TRUE/FALSE and yes/no to nullable boolean; anything else -> null."""
    up = s.astype("string").str.strip().str.upper()
    return up.map(_BOOL_MAP).astype("boolean")


def cast_by_type(s: pd.Series, bqtype: str) -> pd.Series:
    if bqtype == "STRING":
        return cast_string(s)
    if bqtype == "INT64":
        return cast_int64(s)
    if bqtype == "FLOAT64":
        return cast_float64(s)
    if bqtype == "BOOL":
        return cast_bool(s)
    raise ValueError(f"Unknown bigquery_type: {bqtype!r}")


# --------------------------------------------------------------------------- #
# Column-specific normalizations
# --------------------------------------------------------------------------- #
def normalize_candidate_sex(s: pd.Series) -> pd.Series:
    """MALE->M, FEMALE->F, THIRD->O; keep M/F/O; everything else -> null."""
    up = s.astype("string").str.strip().str.upper()
    up = up.replace({"MALE": "M", "FEMALE": "F", "THIRD": "O"})
    return up.where(up.isin(["M", "F", "O"]), other=pd.NA)


def normalize_candidate_type(s: pd.Series) -> pd.Series:
    """GENERAL->GEN; keep GEN/SC/ST; NOTA/OBC/BL/blank/other -> null.

    ECI candidate reservation is only GEN/SC/ST; the rare OBC (n=2) and BL (n=36)
    are source errors and are not in the dictionary, so they are nulled.
    """
    up = s.astype("string").str.strip().str.upper()
    up = up.replace({"GENERAL": "GEN"})
    return up.where(up.isin(["GEN", "SC", "ST"]), other=pd.NA)


def normalize_constituency_type(s: pd.Series) -> pd.Series:
    """Keep GEN/SC/ST; BL/blank -> null."""
    up = s.astype("string").str.strip().str.upper()
    return up.where(up.isin(["GEN", "SC", "ST"]), other=pd.NA)


def load_state_crosswalk() -> dict[str, str]:
    return {
        raw.strip(): code.strip() for raw, code in ref.STATE_CROSSWALK.items()
    }


def map_state_code(
    raw_state: pd.Series, crosswalk: dict[str, str], table: str
) -> pd.Series:
    """Map raw State_Name -> state acronym. Fail loudly on any unmapped value."""
    stripped = raw_state.astype("string").str.strip()
    mapped = stripped.map(crosswalk)
    unmapped_mask = mapped.isna() & stripped.notna() & (stripped != "")
    if unmapped_mask.any():
        bad = sorted(stripped[unmapped_mask].unique().tolist())
        raise ValueError(
            f"[{table}] Unmapped raw State_Name values (not in state_crosswalk.csv): {bad}"
        )
    return mapped.astype("string")


# --------------------------------------------------------------------------- #
# Election-table cleaning
# --------------------------------------------------------------------------- #
def clean_election_table(
    table: str, crosswalk: dict[str, str]
) -> tuple[pd.DataFrame, pa.Schema, pa.Schema]:
    arch = load_architecture(ARCH_FILES[table])
    rename_map = dict(zip(arch["original_name"], arch["name"], strict=True))
    order = arch["name"].tolist()
    type_map = dict(zip(arch["name"], arch["bigquery_type"], strict=True))

    raw = pd.read_csv(
        RAW_FILES[table], dtype=str, keep_default_na=False, compression="gzip"
    )

    # Rename raw -> English; keep only architecture columns (drops Election_Type).
    raw = raw.rename(columns=rename_map)
    missing = [c for c in order if c not in raw.columns]
    if missing:
        raise ValueError(
            f"[{table}] Architecture columns missing from raw data: {missing}"
        )

    out = pd.DataFrame(index=raw.index)
    for name in order:
        bqtype = type_map[name]
        col = raw[name]
        if name == "state_acronym":
            out[name] = map_state_code(col, crosswalk, table)
        elif name == "candidate_sex":
            out[name] = normalize_candidate_sex(col)
        elif name == "candidate_type":
            out[name] = normalize_candidate_type(col)
        elif name == "constituency_type":
            out[name] = normalize_constituency_type(col)
        else:
            # candidate_id (empty -> null) is handled by the generic STRING cast.
            out[name] = cast_by_type(col, bqtype)

    out = out[order]

    # Full schema (for reporting) and file schema (year dropped, hive partition).
    full_schema = pa.schema([pa.field(n, PA_TYPE[type_map[n]]) for n in order])
    file_schema = pa.schema([f for f in full_schema if f.name != "year"])
    return out, full_schema, file_schema


def write_partitioned(
    df: pd.DataFrame, table: str, file_schema: pa.Schema
) -> int:
    """Write one data.parquet per year=<YYYY> partition. Returns partition count."""
    table_dir = OUTPUT_DIR / table
    if table_dir.exists():
        shutil.rmtree(table_dir)
    years = sorted(int(y) for y in df["year"].dropna().unique())
    for yr in years:
        part = df[df["year"] == yr].drop(columns=["year"])
        part_dir = table_dir / f"year={yr}"
        part_dir.mkdir(parents=True, exist_ok=True)
        tbl = pa.Table.from_pandas(
            part, schema=file_schema, preserve_index=False
        )
        pq.write_table(tbl, part_dir / "data.parquet", compression="snappy")
    return len(years)


# --------------------------------------------------------------------------- #
# Reference tables (convert as-is)
# --------------------------------------------------------------------------- #
def write_reference(
    src: pd.DataFrame, table: str, arch_name: str
) -> pd.DataFrame:
    """Type a reference table's columns from its architecture and write parquet."""
    arch = load_architecture(ARCH_DIR / arch_name)
    type_map = dict(zip(arch["name"], arch["bigquery_type"], strict=True))
    df = pd.DataFrame(index=src.index)
    for c in src.columns:
        df[c] = cast_by_type(
            src[c].astype("string"), type_map.get(c, "STRING")
        )
    schema = pa.schema(
        [pa.field(c, PA_TYPE[type_map.get(c, "STRING")]) for c in src.columns]
    )
    out_dir = OUTPUT_DIR / table
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    tbl = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(tbl, out_dir / "data.parquet", compression="snappy")
    return df


def build_state_df() -> pd.DataFrame:
    return pd.DataFrame(ref.STATE_ROWS, columns=ref.STATE_COLUMNS)


def build_dictionary_df() -> pd.DataFrame:
    rows = [
        (t, col, k, "", v)
        for t in ref.DICT_TABLES
        for col, pairs in ref.DICTIONARY.items()
        for k, v in pairs
    ]
    return pd.DataFrame(rows, columns=ref.DICT_COLUMNS)


# --------------------------------------------------------------------------- #
# Reporting
# --------------------------------------------------------------------------- #
def report_election(table: str, df: pd.DataFrame, n_parts: int) -> None:
    print(f"\n{'=' * 70}\n{table}\n{'=' * 70}")
    print(f"rows: {len(df):,}  (expected {EXPECTED_ROWS[table]:,})")
    assert len(df) == EXPECTED_ROWS[table], f"ROW COUNT MISMATCH for {table}"
    print(
        f"year partitions: {n_parts}  "
        f"({int(df['year'].min())}-{int(df['year'].max())})"
    )

    print("\ncolumns + dtypes:")
    for c in df.columns:
        print(f"  {c:<32} {df[c].dtype!s}")

    for c in ["candidate_sex", "candidate_type", "constituency_type"]:
        vals = sorted(df[c].dropna().unique().tolist())
        print(f"\ndistinct {c}: {vals}")

    n_null_state = int(df["state_acronym"].isna().sum())
    print(f"\nnull state_acronym: {n_null_state}  (must be 0)")
    assert n_null_state == 0, f"NULL state_acronym found in {table}"

    n_null_pid = int(df["candidate_id"].isna().sum())
    print(f"null candidate_id: {n_null_pid:,}")
    if n_null_pid:
        # Confirm null-pid rows correspond to NOTA candidates.
        null_rows = df[df["candidate_id"].isna()]
        names_upper = null_rows["candidate_name"].astype("string").str.upper()
        is_nota = names_upper.str.contains(
            "NOTA", na=False
        ) | names_upper.str.contains("NONE OF THE ABOVE", na=False)
        print(
            f"  of which candidate_name indicates NOTA: {int(is_nota.sum()):,} "
            f"/ {n_null_pid:,}"
        )
        non_nota = (
            null_rows[~is_nota]["candidate_name"].dropna().unique().tolist()
        )
        if non_nota:
            print(
                f"  non-NOTA candidate_name values among null pid: {non_nota[:20]}"
            )

    if table == "general_elections":
        print("\n5-row sample:")
        with pd.option_context(
            "display.max_columns", None, "display.width", 240
        ):
            print(df.head(5).to_string())


def report_reference(table: str, df: pd.DataFrame) -> None:
    print(f"\n{'=' * 70}\n{table} (reference, single file)\n{'=' * 70}")
    print(f"rows: {len(df):,}")
    print(f"columns: {list(df.columns)}")


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    crosswalk = load_state_crosswalk()

    for table in ["general_elections", "assembly_elections"]:
        df, _full_schema, file_schema = clean_election_table(table, crosswalk)
        n_parts = write_partitioned(df, table, file_schema)
        report_election(table, df, n_parts)

    st = write_reference(build_state_df(), "state", "state.csv")
    report_reference("state", st)

    dic = write_reference(
        build_dictionary_df(), "dictionary", "dictionary.csv"
    )
    report_reference("dictionary", dic)

    print(f"\n{'=' * 70}\nOutput paths\n{'=' * 70}")
    print(f"  {OUTPUT_DIR / 'general_elections'}/year=<YYYY>/data.parquet")
    print(f"  {OUTPUT_DIR / 'assembly_elections'}/year=<YYYY>/data.parquet")
    print(f"  {OUTPUT_DIR / 'dictionary' / 'data.parquet'}")
    print(f"  {OUTPUT_DIR / 'state' / 'data.parquet'}")


if __name__ == "__main__":
    main()
