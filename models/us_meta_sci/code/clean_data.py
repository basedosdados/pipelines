"""Clean Meta's Social Connectedness Index (SCI) into all-STRING staging parquet.

Usage:
    uv run --no-sync python models/us_meta_sci/code/clean_data.py [table ...]

Reads raw files from ``$US_META_SCI_DATA_DIR/input`` and writes one directory of
snappy parquet per table to ``$US_META_SCI_DATA_DIR/output/<table>/``.
``US_META_SCI_DATA_DIR`` defaults to ``~/Library/Caches/us_meta_sci_data`` (kept
out of the repo and out of Dropbox).

Every source file shares the header
``user_country,friend_country,user_region,friend_region,scaled_sci``. The
transform only renames, drops columns that are redundant for a given file, and
adds a level column for files that bundle several geographies. Values are never
altered. Two traps are guarded:

* Namibia's ISO2 code is ``NA``. The CSV is read with every column as VARCHAR and
  only the empty string as NULL, so ``NA`` survives as a country code.
* Leading zeros in county FIPS and ZCTA codes survive for the same reason.

Each table's output directory is deleted before it is rewritten, so a re-run never
keeps parquet produced by an older version of this code. A manifest with row
counts and checks is written to ``output/_manifest_<table>.json``.
"""

import json
import os
import shutil
import sys
import zipfile
from pathlib import Path

import duckdb

DATA_DIR = Path(
    os.environ.get(
        "US_META_SCI_DATA_DIR",
        Path.home() / "Library" / "Caches" / "us_meta_sci_data",
    )
)
INPUT = DATA_DIR / "input"
OUTPUT = DATA_DIR / "output"
TMP = DATA_DIR / "tmp"
MEMORY_LIMIT = os.environ.get("US_META_SCI_MEMORY_LIMIT", "12GB")
THREADS = int(os.environ.get("US_META_SCI_THREADS", "8"))

# Columns of each table, as SQL expressions over the raw CSV, in architecture order.
PAIR_COLS = [
    ("user_country", "user_country_id"),
    ("friend_country", "friend_country_id"),
    ("user_region", "user_region_id"),
    ("friend_region", "friend_region_id"),
    ("scaled_sci", "scaled_sci"),
]
COUNTRY_COLS = [
    ("user_country", "user_country_id"),
    ("friend_country", "friend_country_id"),
    ("scaled_sci", "scaled_sci"),
]
COUNTY_COLS = [
    ("user_region", "user_county_id"),
    ("friend_region", "friend_county_id"),
    ("scaled_sci", "scaled_sci"),
]
ZCTA_COLS = [
    ("user_region", "user_zcta_id"),
    ("friend_region", "friend_zcta_id"),
    ("scaled_sci", "scaled_sci"),
]
REGION_TO_COUNTRY_COLS = [
    ("user_country", "user_country_id"),
    ("user_region", "user_region_id"),
    ("friend_country", "friend_country_id"),
    ("scaled_sci", "scaled_sci"),
]

# table -> (source file, member filter, columns, level column name, member->level)
TABLES = {
    "country": ("country.csv", None, COUNTRY_COLS, None, None),
    "gadm1": ("gadm1.csv", None, PAIR_COLS, None, None),
    "gadm2": ("gadm2.zip", None, PAIR_COLS, None, None),
    "geoboundaries_adm1": (
        "geoboundaries_adm1.csv",
        None,
        PAIR_COLS,
        None,
        None,
    ),
    "geoboundaries_adm2": (
        "geoboundaries_adm2.zip",
        None,
        PAIR_COLS,
        None,
        None,
    ),
    "us_county": ("us_counties.csv", None, COUNTY_COLS, None, None),
    "us_zcta": ("us_zcta.zip", None, ZCTA_COLS, None, None),
    "nuts_2024": (
        "nuts_2024.zip",
        None,
        PAIR_COLS,
        "nuts_level",
        lambda stem: stem.replace("_2024", ""),  # nuts1_2024 -> nuts1
    ),
    "region_to_country": (
        "all_region_to_country.zip",
        None,
        REGION_TO_COUNTRY_COLS,
        "region_level",
        lambda stem: stem.replace("_to_country", "").replace(
            "us_counties", "us_county"
        ),
    ),
}

# Columns that must match the friend side's code when the table is a region->country file.
ZERO_FRIEND_REGION_CHECK = {"region_to_country"}


def _members(zpath: Path) -> list[str]:
    with zipfile.ZipFile(zpath) as z:
        return sorted(
            n
            for n in z.namelist()
            if n.lower().endswith(".csv")
            and not n.startswith("__MACOSX")
            and "/._" not in n
        )


def _extract(zpath: Path, member: str) -> Path:
    TMP.mkdir(parents=True, exist_ok=True)
    out = TMP / Path(member).name
    with (
        zipfile.ZipFile(zpath) as z,
        z.open(member) as src,
        open(out, "wb") as dst,
    ):
        shutil.copyfileobj(src, dst, length=16 * 1024 * 1024)
    return out


def _convert(
    con, csv: Path, outdir: Path, stem: str, cols, level_col, level_value
) -> dict:
    """Write one CSV to parquet; return checks on the raw file."""
    read = (
        f"read_csv('{csv}', header=true, all_varchar=true, nullstr='', "
        "delim=',', quote='\"', strict_mode=true)"
    )
    con.execute(f"create or replace temp view raw as select * from {read}")
    hdr = [r[0] for r in con.execute("describe raw").fetchall()]
    expected = [
        "user_country",
        "friend_country",
        "user_region",
        "friend_region",
        "scaled_sci",
    ]
    if hdr != expected:
        raise ValueError(f"{csv.name}: unexpected header {hdr}")

    checks = con.execute(
        """
        select
            count(*) as n,
            count(*) filter (where try_cast(scaled_sci as bigint) is null) as bad_sci,
            min(try_cast(scaled_sci as bigint)) as min_sci,
            max(try_cast(scaled_sci as bigint)) as max_sci,
            count(*) filter (where user_country is null or friend_country is null
                or user_region is null or friend_region is null) as null_keys,
            count(distinct user_region) as n_user_regions,
            count(distinct friend_region) as n_friend_regions,
            count(*) filter (where user_country = friend_country
                and user_region = friend_region and user_region <> user_country) as self_pairs,
            count(*) filter (where friend_region <> friend_country) as friend_region_ne_country
        from raw
        """
    ).fetchone()
    names = [
        "rows",
        "bad_sci",
        "min_sci",
        "max_sci",
        "null_keys",
        "n_user_regions",
        "n_friend_regions",
        "self_pairs",
        "friend_region_ne_country",
    ]
    res = dict(zip(names, checks, strict=True))
    if res["bad_sci"] or res["null_keys"]:
        raise ValueError(f"{csv.name}: invalid rows {res}")

    select = []
    if level_col:
        select.append(f"'{level_value}' as {level_col}")
    select += [f"{src} as {dst}" for src, dst in cols]
    con.execute(
        f"copy (select {', '.join(select)} from raw) to '{outdir}' "
        f"(format parquet, compression snappy, file_size_bytes '512MB', "
        f"filename_pattern '{stem}_{{i}}', overwrite_or_ignore true)"
    )
    return res


def clean(table: str, delete_archive: bool = False) -> dict:
    src, _, cols, level_col, level_of = TABLES[table]
    path = INPUT / src
    outdir = OUTPUT / table
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True)

    con = duckdb.connect()
    # Bound RAM: DuckDB defaults to 80% of physical memory, and the adm2 shards
    # (100M+ rows each) spill to TMP instead of exhausting the machine.
    con.execute(f"set memory_limit='{MEMORY_LIMIT}'")
    con.execute(f"set threads={THREADS}")
    con.execute(f"set temp_directory='{TMP}'")
    con.execute("set preserve_insertion_order=false")

    parts = {}
    if path.suffix == ".zip":
        for m in _members(path):
            stem = Path(m).stem
            csv = _extract(path, m)
            try:
                lvl = level_of(stem) if level_of else None
                parts[stem] = _convert(
                    con, csv, outdir, stem, cols, level_col, lvl
                )
            finally:
                csv.unlink(missing_ok=True)
            print(
                f"  {table}/{stem}: {parts[stem]['rows']:,} rows", flush=True
            )
    else:
        parts[path.stem] = _convert(
            con, path, outdir, path.stem, cols, None, None
        )

    total = sum(p["rows"] for p in parts.values())
    row = con.execute(
        f"select count(*) from read_parquet('{outdir}/*.parquet')"
    ).fetchone()
    out_rows = row[0] if row else 0
    if out_rows != total:
        raise ValueError(f"{table}: wrote {out_rows:,} rows, read {total:,}")
    manifest = {"table": table, "source": src, "rows": total, "parts": parts}
    (OUTPUT / f"_manifest_{table}.json").write_text(
        json.dumps(manifest, indent=2)
    )
    print(f"{table}: {total:,} rows -> {outdir}", flush=True)
    if delete_archive and path.suffix == ".zip":
        path.unlink()
        print(f"  deleted archive {path.name}", flush=True)
    return manifest


if __name__ == "__main__":
    args = sys.argv[1:]
    delete = "--delete-archive" in args
    tables = [a for a in args if not a.startswith("--")] or list(TABLES)
    for t in tables:
        clean(t, delete_archive=delete)
