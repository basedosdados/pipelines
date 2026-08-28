"""Convert Minas Gerais source files to all-STRING staging parquet.

MG publishes a documented dimensional model (Frictionless) over SIAFI/MG: per-year fact
tables, a per-year empenho document dimension, and ~15 shared dimension tables, plus a
separate procurement model (`compras_contratos`) that carries a native process->empenho
bridge.

This step deliberately does **not** harmonize. It mirrors each source table into staging
with every column as STRING, and the joins onto the canonical `despesa` schema happen in
dbt. That keeps ~100M fact rows out of pandas and puts the mapping somewhere reviewable.

Staging is all-STRING by house convention, and it must be all-STRING here specifically:
the recurring-pipeline upload path stringifies its header, so a typed external table left
behind by onboarding collides with the pipeline's later overwrite. See
.claude/rules/prefect-pipeline-conventions.md, "Staging parquet must be all-STRING".
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from constants import (
    INPUT_DIR,
    MG_SEP,
    MG_STATIC_TABLES,
    MG_YEARLY_TABLES,
    OUTPUT_DIR,
)

MG_INPUT = INPUT_DIR / "mg"

# Which column carries the year, per yearly source table. MG partitions its fact table on
# `ano_particao`, which is NOT always the year in the file name -- restos a pagar movements
# carry the original exercise. Trust the column, never the file name.
YEAR_COLUMN = {
    "ft_despesa": "ano_particao",
    "dm_empenho_desp": "ano_exercicio",
}


def _read_all_varchar(
    con: duckdb.DuckDBPyConnection, path: Path, union_by_name: bool = False
) -> str:
    """A duckdb relation over one source CSV, every column forced to VARCHAR.

    `all_varchar` is what keeps staging faithful: no silent numeric coercion, no locale
    surprises on the BR decimal format, and NULL stays NULL rather than becoming "nan"
    (which safe_cast would not turn back into NULL downstream).
    """
    # duckdb strips the UTF-8 BOM itself; passing encoding='utf-8-sig' is rejected.
    #
    # `quote` must be pinned. Most MG rows are unquoted, so the sniffer concludes there is
    # no quote character -- and then a legitimately quoted field containing the delimiter
    # blows up the row, e.g. in dm_favorecido:
    #     994503;1;***.195.606-**;"IVAN BARBOSA DE OLIVEIRA- N NIT;11286800336"
    # which reads as 5 columns instead of 4. Strict mode stays on so that any *other*
    # malformed row is a loud failure rather than a silently dropped record.
    extra = ", union_by_name=true" if union_by_name else ""
    return (
        f"read_csv('{path}', delim='{MG_SEP}', header=true, all_varchar=true, "
        f"quote='\"', escape='\"', ignore_errors=false{extra})"
    )


def clean_static(con: duckdb.DuckDBPyConnection, stem: str, table: str) -> int:
    src = MG_INPUT / f"{stem}.csv.gz"
    if not src.exists():
        print(f"  SKIP {stem}: not downloaded")
        return 0
    dest = OUTPUT_DIR / table
    dest.mkdir(parents=True, exist_ok=True)
    rel = _read_all_varchar(con, src)
    con.execute(
        f"COPY (SELECT * FROM {rel}) TO '{dest / 'data.parquet'}' "
        "(FORMAT PARQUET, COMPRESSION SNAPPY)"
    )
    n = con.execute(f"SELECT count(*) FROM {rel}").fetchone()[0]
    print(f"  {table}: {n:,} rows")
    return n


def clean_yearly(con: duckdb.DuckDBPyConnection, stem: str, table: str) -> int:
    srcs = sorted(MG_INPUT.glob(f"{stem}_*.csv.gz"))
    if not srcs:
        print(f"  SKIP {stem}: not downloaded")
        return 0
    year_col = YEAR_COLUMN[stem]
    dest = OUTPUT_DIR / table
    dest.mkdir(parents=True, exist_ok=True)
    # Clear previous output, including any leftover hive-partitioned `ano=YYYY/` layout
    # from an earlier run -- a stale partition dir would otherwise be picked up by the
    # uploader's wildcard alongside the new flat files and double-count every row.
    for stale in dest.rglob("*.parquet"):
        stale.unlink()
    for sub in sorted(dest.glob("ano=*"), reverse=True):
        if sub.is_dir():
            sub.rmdir()

    # Deliberately NOT hive-partitioned, and `ano` is kept as a real column.
    #
    # Hive partitioning would push `ano` into the directory name, which only survives as a
    # column if the staging table is created as an *external* table -- i.e. via
    # bd.Table.create, which reads the whole parquet into pandas and stringifies it. On an
    # 80M-row fact table that balloons to tens of GB of RAM and can wedge the machine
    # (see .claude memory: reference_bd_table_create_ram_blowup). Keeping `ano` in the file
    # lets upload.py stream straight to GCS and load server-side, at flat RAM.
    #
    # One output parquet per SOURCE file, in a single pass. Splitting by distinct `ano`
    # instead would mean re-scanning all 25 gzipped CSVs once per year -- quadratic, and
    # measurably so on a 1.3 GB fact table. Because `ano` is carried inside the file, the
    # file boundaries carry no meaning downstream, so they may as well follow the input.
    # (A source file is mostly one exercise but not purely: restos a pagar movements keep
    # their original ano_particao, which is exactly why the column is authoritative and
    # the file name is not.)
    total = 0
    for src in srcs:
        # ft_despesa_2002.csv.gz -> data_2002.parquet (Path.stem strips only ".gz")
        year_tag = src.name.removesuffix(".csv.gz").rsplit("_", 1)[-1]
        out = dest / f"data_{year_tag}.parquet"
        rel = _read_all_varchar(con, src)
        # `ano` is normalised through INTEGER and back to VARCHAR, not left as an integer.
        # Staging is all-STRING by house convention, and mixing one typed column in is the
        # exact shape of a known failure: a recurring pipeline later writes all-STRING
        # parquet over the same prefix, and dbt then reads a stale typed external table
        # against string files ("Invalid cast from INT64 to DATE"). The round trip through
        # INTEGER is what strips a stray "2013.0" so safe_cast(ano as int64) still works.
        con.execute(
            f"COPY (SELECT * EXCLUDE ({year_col}), "
            f"       CAST(CAST({year_col} AS INTEGER) AS VARCHAR) AS ano "
            f"      FROM {rel} WHERE {year_col} IS NOT NULL) "
            f"TO '{out}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
        )
        n = con.execute(
            f"SELECT count(*) FROM read_parquet('{out}')"
        ).fetchone()[0]
        total += n
        print(f"    {src.name}: {n:,} rows", flush=True)

    span = con.execute(
        f"SELECT min(CAST(ano AS INTEGER)), max(CAST(ano AS INTEGER)), "
        f"       count(DISTINCT ano) "
        f"FROM read_parquet('{dest}/data_*.parquet')"
    ).fetchone()
    print(
        f"  {table}: {total:,} rows across {len(srcs)} files "
        f"-> exercises {span[0]}-{span[1]} ({span[2]} distinct)"
    )
    return total


def main(only: str | None = None) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    con.execute("PRAGMA memory_limit='6GB'")

    print("MG yearly tables")
    for stem, table in MG_YEARLY_TABLES.items():
        if only and only != table:
            continue
        clean_yearly(con, stem, table)

    print("MG static tables")
    for stem, table in MG_STATIC_TABLES.items():
        if only and only != table:
            continue
        clean_static(con, stem, table)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="build a single staging table")
    main(**vars(ap.parse_args()))
