"""Clean the Minas Gerais `portal_*` flat exports into staging parquet.

One output table per stem in `MG_PORTAL_TABLES`, unioning the annual CSVs. Every column
is forced to VARCHAR, as everywhere else in this dataset: staging mirrors the source and
the dbt model does the casting, so no numeric coercion or locale guessing happens here.

The year files are uniform. Verified on `portal_contratos` at
3998d827: contratos2022 through contratos2026 all carry the same 24 columns in the same
order. `union_by_name` is still used -- it costs nothing and makes a future upstream
column addition a widened table rather than a silently shifted one.

Two things NOT to do here, both learned from the source rather than assumed:

  * Do not drop `unnamed_*` columns. They exist only in the repos'
    `dataset/datapackage.json`, which is generated from the upstream Excel and is stale.
    The published CSVs have none. (The same schemas also omit a real column,
    `indicador_fornecedor_estrangeiro`, so they are not a reliable column list either
    way.)
  * Do not slice columns positionally. The repos' own `processar.py` does
    `df.iloc[:, 1:]` against the Excel; applied to these CSVs it would silently discard
    `ano_assinatura_contrato`.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from models.br_bd_execucao_estadual.code.constants import (
    INPUT_DIR,
    MG_PORTAL_IN_USE,
    MG_PORTAL_TABLES,
    MG_SEP,
    OUTPUT_DIR,
)

MG_INPUT = INPUT_DIR / "mg"


def _read_all_varchar(paths: list[Path]) -> str:
    """A duckdb relation over the annual CSVs, every column forced to VARCHAR.

    `quote` and `escape` are pinned for the same reason as in `clean_mg.py`: most rows
    are unquoted, so the sniffer can conclude there is no quote character and then a
    legitimately quoted `objeto_contrato` containing a semicolon splits the row. Strict
    mode stays on so any other malformed row fails loudly instead of vanishing.
    """
    files = ", ".join(f"'{p}'" for p in paths)
    return (
        f"read_csv([{files}], delim='{MG_SEP}', header=true, all_varchar=true, "
        "quote='\"', escape='\"', ignore_errors=false, union_by_name=true)"
    )


def clean(con: duckdb.DuckDBPyConnection, stem: str, table: str) -> int:
    srcs = sorted(MG_INPUT.glob(f"{stem}[0-9][0-9][0-9][0-9].csv"))
    if not srcs:
        print(f"  SKIP {stem}: not downloaded")
        return 0
    dest = OUTPUT_DIR / table
    dest.mkdir(parents=True, exist_ok=True)
    # Clear previous output, including any leftover hive-partitioned layout from an
    # earlier run -- a stale partition dir would be picked up by the uploader's wildcard
    # alongside the new file and double-count every row.
    for stale in dest.rglob("*.parquet"):
        stale.unlink()
    for sub in sorted(dest.glob("ano=*"), reverse=True):
        if sub.is_dir():
            sub.rmdir()

    rel = _read_all_varchar(srcs)
    con.execute(
        f"COPY (SELECT * FROM {rel}) TO '{dest / 'data.parquet'}' "
        "(FORMAT PARQUET, COMPRESSION SNAPPY)"
    )
    # pyrefly: ignore [unsupported-operation]
    n = con.execute(f"SELECT count(*) FROM {rel}").fetchone()[0]
    print(f"  {table}: {n:,} rows from {len(srcs)} file(s)")
    return n


def main(only: str | None = None) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    stems = [only] if only else list(MG_PORTAL_IN_USE)
    unknown = [s for s in stems if s not in MG_PORTAL_TABLES]
    if unknown:
        raise SystemExit(
            f"unknown stem(s) {unknown}; known: {sorted(MG_PORTAL_TABLES)}"
        )
    con = duckdb.connect()
    total = 0
    for stem in stems:
        total += clean(con, stem, MG_PORTAL_TABLES[stem])
    print(f"  total: {total:,} rows")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--only",
        help=f"one stem of {sorted(MG_PORTAL_TABLES)} (default: those in use)",
    )
    args = ap.parse_args()
    main(only=args.only)
