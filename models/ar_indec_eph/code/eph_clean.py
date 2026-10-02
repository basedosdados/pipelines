"""Cleaning transform for ar_indec_eph: 87 raw wave archives -> partitioned parquet.

Pure functions, no Prefect imports, so the recurring pipeline under
pipelines/datasets/ar_indec_eph/ can import this module rather than duplicate it.

What the transform has to reconcile across the four source eras:

1. Format. Stata (.dta, zipped or RAR'd) for 2003 Q3 - 2015 Q2; semicolon TXT
   for 2016 Q2 on. archives.py hides the packaging difference.
2. Decimal separator. The TXT era writes decimals with a comma.
3. Stata float storage. Stata stores small integers as floats, so a code column
   arrives as "1.0" where the TXT era writes "1".
4. Zero padding. INDEC padded its code columns until some point between 2016 and
   2024 and then stopped, so one decile group is "03" early and "3" late.
   pad_widths.json records the canonical width per column; see analyse_padding.py.
5. Column drift. The questionnaire changed, so each wave carries its own subset
   of the 245/103 column union. Missing columns are emitted as NULL.
6. Two empty source artefacts, dropped: CH142 (2009 Q4 only, a single distinct
   value) and an unnamed trailing column created by a stray semicolon in 2021 Q3.

Output is all-STRING parquet, per .claude/rules/bigquery-conventions.md: staging
is all-STRING by house convention and the dbt model safe_casts every column. The
cast goes through arrow after the architecture's real types have been applied, so
NULL stays NULL (astype(str) would write the literal "nan") and an integer-valued
float serialises as "2003" rather than "2003.0".
"""

import csv
import json
import shutil
import tempfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from models.ar_indec_eph.code.archives import (
    data_members,
    extract,
    read_dta,
)
from models.ar_indec_eph.code.constants import (
    ARCH_DIR,
    CODE_DIR,
    OUTPUT_DIR,
    TABLES,
    waves,
)

PARTITION_COLS = ["ano", "trimestre"]
NULL_TOKENS = {"", "nan", "NaN", "None", "NA", "."}

# The architecture's BigQuery types, as arrow types. Values are built with these
# and only then cast to string, which is what keeps an integral amount as
# "140000" rather than pandas' "140000.0", and a NULL as NULL rather than the
# literal "nan" that astype(str) would write.
ARROW_TYPE = {
    "STRING": pa.string(),
    "INT64": pa.int64(),
    "FLOAT64": pa.float64(),
}


def load_architecture(table: str) -> list[dict]:
    with open(ARCH_DIR / f"{table}.csv", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_pad_widths() -> dict[str, dict[str, int]]:
    path = CODE_DIR / "pad_widths.json"
    if not path.exists():
        return {t: {} for t in TABLES}
    return json.loads(path.read_text(encoding="utf-8"))["widths"]


def load_drops() -> dict[str, set]:
    over = json.loads(
        (CODE_DIR / "overrides.json").read_text(encoding="utf-8")
    )
    return {t: set(over.get("_drop", {}).get(t) or {}) for t in TABLES}


def read_raw(wave: dict, member: str) -> pd.DataFrame:
    """Read one wave's table as strings, with source names uppercased."""
    tmp = Path(tempfile.mkdtemp(prefix="eph_clean_"))
    try:
        path = extract(wave, member, tmp)
        if wave["fmt"] == "dta":
            frame, _meta = read_dta(path)
            # Stata stores codes as floats; render integral floats without the
            # trailing ".0" before everything becomes a string.
            for col in frame.columns:
                if pd.api.types.is_float_dtype(frame[col]):
                    integral = frame[col].dropna() % 1 == 0
                    if bool(integral.all()):
                        frame[col] = frame[col].astype("Int64")
            return frame.astype("string")
        frame = pd.read_csv(
            path, sep=";", encoding="latin-1", dtype=str, low_memory=False
        )
        frame.columns = [
            str(c).strip().strip('"').upper() for c in frame.columns
        ]
        return frame.astype("string")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def normalise(series: pd.Series) -> pd.Series:
    s = series.str.strip().str.strip('"').str.strip()
    # pyrefly: ignore [bad-argument-type]  pandas-stubs omits pd.NA from the
    # Scalar union accepted by to_replace, though replace() takes it.
    return s.replace({t: pd.NA for t in NULL_TOKENS})


def to_number(series: pd.Series) -> pd.Series:
    """Parse a numeric column, accepting the TXT era's decimal comma."""
    s = series.str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")


def clean_wave(
    wave: dict, table: str, arch: list[dict], pad: dict[str, int], drops: set
) -> pd.DataFrame:
    member = data_members(wave)[table]
    raw = read_raw(wave, member)

    out = pd.DataFrame(index=raw.index)
    for row in arch:
        src, name, btype = (
            row["original_name"],
            row["name"],
            row["bigquery_type"],
        )
        if src in drops:
            continue
        if src not in raw.columns:
            out[name] = pd.Series(pd.NA, index=raw.index, dtype="string")
            continue
        col = normalise(raw[src])
        if btype in ("INT64", "FLOAT64"):
            num = to_number(col)
            if btype == "INT64":
                out[name] = num.round().astype("Int64")
            else:
                out[name] = num.astype("Float64")
        else:
            width = pad.get(src)
            if width:
                # Only pad values that are purely numeric and short; a value
                # already at full width, or non-numeric, is left alone.
                mask = (
                    col.notna()
                    & col.str.fullmatch(r"\d+")
                    & (col.str.len() < width)
                )
                col = col.mask(mask, col.str.zfill(width))
            out[name] = col.astype("string")

    # The partition values come from the manifest, not the file, so a wave whose
    # ANO4/TRIMESTRE disagree with its own filename cannot scatter rows.
    out["ano"] = pd.Series(wave["year"], index=raw.index, dtype="Int64")
    out["trimestre"] = pd.Series(
        wave["quarter"], index=raw.index, dtype="Int64"
    )
    return out


def write_partition(
    frame: pd.DataFrame, table: str, wave: dict, arch: list[dict], drops: set
) -> Path:
    kept = [r for r in arch if r["original_name"] not in drops]
    order = [r["name"] for r in kept]
    frame = frame[order]
    typed_schema = pa.schema(
        [pa.field(r["name"], ARROW_TYPE[r["bigquery_type"]]) for r in kept]
    )
    string_schema = pa.schema([pa.field(name, pa.string()) for name in order])
    dest = (
        OUTPUT_DIR
        / table
        / f"ano={wave['year']}"
        / f"trimestre={wave['quarter']}"
    )
    dest.mkdir(parents=True, exist_ok=True)
    table_arrow = pa.Table.from_pandas(
        frame, schema=typed_schema, preserve_index=False
    )
    table_arrow = table_arrow.cast(string_schema)
    path = dest / "data.parquet"
    pq.write_table(table_arrow, path, compression="snappy")
    return path


def clean_all(only: list[tuple[int, int]] | None = None) -> dict[str, int]:
    arch = {t: load_architecture(t) for t in TABLES}
    pad = load_pad_widths()
    drops = load_drops()
    totals = {t: 0 for t in TABLES}
    selected = waves()
    if only:
        wanted = set(only)
        selected = [w for w in selected if (w["year"], w["quarter"]) in wanted]

    for wave in selected:
        line = [f"{wave['year']}Q{wave['quarter']}"]
        for table in TABLES:
            frame = clean_wave(
                wave, table, arch[table], pad.get(table, {}), drops[table]
            )
            write_partition(frame, table, wave, arch[table], drops[table])
            totals[table] += len(frame)
            line.append(f"{table.split('_')[-1]}={len(frame)}")
        print("  ".join(line), flush=True)
    return totals


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    totals = clean_all()
    print("\n=== totals ===")
    for table, n in totals.items():
        print(f"{table}: {n:,} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
