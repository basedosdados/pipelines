"""Rename the geography columns in the already-cleaned parquet, in place, memory-bounded.

The static clean already wrote parquet with the pre-rename names (state_code, county_code,
census_tract, derived_msa_md / msa_md). This rewrites each file with the final names so the
staging bucket (which feeds prod materialization) and any re-upload carry the correct names.
The recurring pipeline reuses clean.py, which now emits the new names directly from the
updated architecture, so this one-off patch is only for the existing static export.

  uv run --with duckdb python rename_parquet.py
"""

import glob
from pathlib import Path

import duckdb
from common import LEGACY, MODERN, OUTPUT

RENAME = {
    MODERN: {
        "state_code": "state_abbreviation",
        "county_code": "county_id",
        "census_tract": "census_tract_id",
        "derived_msa_md": "msa_md_id",
    },
    LEGACY: {
        "state_code": "state_id",
        "county_code": "county_id",
        "census_tract": "census_tract_id",
        "msa_md": "msa_md_id",
    },
}


def rewrite(f: str, rmap: dict) -> None:
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='4GB'")
    con.execute(f"SET temp_directory='{OUTPUT.parent / 'duck_tmp'}'")
    cols = [
        r[0]
        for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{f}')"
        ).fetchall()
    ]
    present = {o: n for o, n in rmap.items() if o in cols}
    if not present:
        print(f"  no rename needed: {f}")
        con.close()
        return
    sel = ", ".join(
        f'"{c}" AS {present[c]}' if c in present else f'"{c}"' for c in cols
    )
    tmp = f + ".tmp"
    con.execute(
        f"COPY (SELECT {sel} FROM read_parquet('{f}')) "
        f"TO '{tmp}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)"
    )
    con.close()
    Path(tmp).replace(f)
    print(
        f"  renamed {list(present.items())} in {Path(f).parent.name}/{Path(f).name}"
    )


def main() -> None:
    for table, rmap in ((MODERN, RENAME[MODERN]), (LEGACY, RENAME[LEGACY])):
        files = sorted(
            glob.glob(str(OUTPUT / table / "**" / "*.parquet"), recursive=True)
        )
        print(f"{table}: {len(files)} files")
        for f in files:
            rewrite(f, rmap)
    print("rename complete.")


if __name__ == "__main__":
    main()
