"""Turn the downloaded SDMX CSV chunks into partitioned, all-STRING parquet.

Reads the architecture CSVs for column names, order and the SDMX -> clean name
mapping, so nothing here declares a schema of its own.

What it does per table, in order:

1. Rename the SDMX components to their Data Basis names.
2. Derive ``year`` -- from ``TIME_PERIOD`` where the cube has a time dimension,
   and otherwise from the ``REF_PERIOD`` attribute, so that all 15 tables
   partition the same way.
3. Derive ``country_iso3_code``: ``REF_AREA`` mixes countries, subnational
   entities and aggregates, and exactly the 249 CL_AREA codes that are ISO-3
   countries appear in ``country_iso3.csv``. Everything else -- ``OECD``,
   ``EU25``, ``G20``, the subnational entities -- stays null.
4. Drop rows with no ``obs_value``. The API returns the full cross-product, so
   55% of downloaded rows carry no value at all, only an ``obs_status`` saying
   why. Keeping them would make ``finance`` a 27.9M-row table that is 71% empty.
5. Deduplicate. Only matters for the tables that union flow versions, where the
   newest version wins on a tie.

Staging parquet is written **all-STRING**, cast through arrow rather than
``astype(str)`` -- the latter renders NULL as the literal "nan", which
``safe_cast`` will not turn back into NULL. The dbt model casts each column to
its architecture type.

Run: ``python clean.py``           all tables
     ``python clean.py student``   one table
"""

import csv
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from common import ARCH_DIR, CODE_DIR, INPUT, OUTPUT
from tables import TABLES


def architecture(slug):
    """[(clean_name, sdmx_name)] in architecture order."""
    with (ARCH_DIR / f"{slug}.csv").open() as f:
        return [(r["name"], r["original_name"]) for r in csv.DictReader(f)]


def countries():
    with (CODE_DIR / "country_iso3.csv").open() as f:
        return {r["sigla_iso3"] for r in csv.DictReader(f)}


ISO3 = countries()


def clean_chunk(df, slug, columns, flow, version):
    """One downloaded chunk -> the table's clean column set."""
    rename = {
        sdmx: name for name, sdmx in columns if sdmx and sdmx in df.columns
    }
    out = df.rename(columns=rename)

    # year: TIME_PERIOD for cubes that have one, REF_PERIOD for those that do not.
    year_source = dict(columns).get("year")
    if year_source == "REF_PERIOD" and "reference_period" in out.columns:
        out["year"] = out["reference_period"]
    out["year"] = pd.to_numeric(out.get("year"), errors="coerce").astype(
        "Int64"
    )

    out["country_iso3_code"] = out["reference_area"].where(
        out["reference_area"].isin(ISO3)
    )
    out["source_flow"] = flow
    out["source_flow_version"] = version

    # The cross-product padding: no value, only a status saying why not.
    out = out[
        out["obs_value"].notna()
        & (out["obs_value"].astype(str).str.strip() != "")
    ]

    for name, _ in columns:
        if name not in out.columns:
            out[name] = pd.NA
    return out[[name for name, _ in columns]]


def to_string_table(df, columns):
    """All-STRING arrow table with a stable column order.

    Cast through arrow, never ``astype(str)``: that renders NULL as "nan", and
    it would serialise year as "2013.0" rather than "2013".
    """
    arrays = []
    for name, _ in columns:
        col = df[name]
        if str(col.dtype).startswith(("Int", "int", "float", "Float")):
            arr = pa.array(col, from_pandas=True).cast(pa.string())
        else:
            arr = pa.array(
                col.astype(object), type=pa.string(), from_pandas=True
            )
        arrays.append(arr)
    return pa.Table.from_arrays(arrays, names=[n for n, _ in columns])


def stacks_editions(spec):
    """Whether this table unions more than one flow version."""
    return (
        bool(spec.get("stack"))
        or bool(spec.get("backfill"))
        or len(spec["versions"]) > 1
    )


def chunk_frames(slug, spec, columns):
    """(year, cleaned frame) for every non-empty downloaded chunk."""
    src = INPUT / slug
    chunks = sorted(src.glob("*.csv"))
    if not chunks:
        raise FileNotFoundError(f"no downloaded chunks for {slug} in {src}")
    for path in chunks:
        with path.open(errors="ignore") as f:
            if f.read(16).startswith("# NoRecordsFound"):
                continue
        df = pd.read_csv(path, dtype=str, low_memory=False)
        if df.empty:
            continue
        tokens = path.stem.split("_")
        # "<FLOW>_<version>[_<year>]" -- the version is the token containing a dot.
        version = next(
            t for t in tokens if "." in t and t.replace(".", "").isdigit()
        )
        yield path, clean_chunk(df, slug, columns, spec["flow"], version)


def write_partitions(df, columns, out_dir):
    """Write one parquet per year. Returns rows written."""
    total = 0
    for year, part in df.groupby("year", dropna=True):
        d = out_dir / f"year={int(year)}"
        d.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            to_string_table(part, columns),
            d / "data.parquet",
            compression="snappy",
        )
        total += len(part)
    return total


def clean_table(slug, spec):
    """Clean one table, streaming chunk by chunk where that is safe.

    The download is already chunked by year and a year maps to exactly one
    partition, so a single-edition table never needs more than one chunk in
    memory -- which matters, because concatenating finance whole would be 27.9M
    rows of strings. Only the tables that union editions need a cross-chunk
    view, and every one of those is small (the largest, salary_trend, is 26,040
    observations).
    """
    columns = architecture(slug)
    out_dir = OUTPUT / slug
    total, years = 0, set()

    if stacks_editions(spec):
        frames = [df for _, df in chunk_frames(slug, spec, columns)]
        if not frames:
            print(f"  {slug}: no rows with a value")
            return 0
        df = pd.concat(frames, ignore_index=True)
        # Newest edition wins where two describe the same cell.
        key = [
            n
            for n, sdmx in columns
            if sdmx and n not in ("obs_value", "source_flow_version")
        ]
        key = [k for k in key if k in df.columns]
        before = len(df)
        df = df.sort_values("source_flow_version").drop_duplicates(
            subset=key, keep="last"
        )
        if before - len(df):
            print(f"  {slug}: {before - len(df):,} duplicate cells collapsed")
        total = write_partitions(df, columns, out_dir)
        years = set(df["year"].dropna().astype(int))
    else:
        for _path, df in chunk_frames(slug, spec, columns):
            if df.empty:
                continue
            total += write_partitions(df, columns, out_dir)
            years |= set(df["year"].dropna().astype(int))
            del df

    print(f"  {slug}: {total:,} rows in {len(years)} partitions")
    return total


def main():
    wanted = sys.argv[1:] or list(TABLES)
    grand = 0
    for slug in wanted:
        grand += clean_table(slug, TABLES[slug])
    print(f"\n{len(wanted)} tables, {grand:,} rows")


if __name__ == "__main__":
    main()
