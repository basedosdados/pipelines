"""Clean the Minas Gerais `portal_*` flat exports into staging parquet.

One output table per stem in `MG_PORTAL_TABLES`, unioning the annual CSVs. Every column
is forced to VARCHAR, as everywhere else in this dataset: staging mirrors the source and
the dbt model does the casting, so no numeric coercion or locale guessing happens here.

The year files are uniform. Verified on `portal_contratos` at
3998d827: contratos2022 through contratos2026 all carry the same 24 columns in the same
order. `union_by_name` is still used -- it costs nothing and makes a future upstream
column addition a widened table rather than a silently shifted one.

`unnamed_*` columns are dropped, but do NOT use the repos' `dataset/datapackage.json` to
decide which exist. Those schemas are generated from the upstream Excel and disagree with
the published CSVs in both directions: they declare `unnamed_*` columns that are absent
(`contratos2024.csv` has 24 real columns against 28 in its schema, `itens` 13 against 16)
and they omit a column that is present (`indicador_fornecedor_estrangeiro`). The dropping
below is driven by the actual CSV header. As of the pinned refs only
`fiscais_contratos_2022.csv` carries one, `unnamed_17`.

Do not slice columns positionally. The repos' own `processar.py` does `df.iloc[:, 1:]`
against the Excel; applied to these CSVs it would silently discard the first real column
(`ano_assinatura_contrato`).
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import duckdb

from models.br_bd_execucao_estadual.code.constants import (
    INPUT_DIR,
    MG_PORTAL_IN_USE,
    MG_PORTAL_LISTED_IN_USE,
    MG_PORTAL_LISTED_TABLES,
    MG_PORTAL_MONTHLY_IN_USE,
    MG_PORTAL_MONTHLY_TABLES,
    MG_PORTAL_STATIC_IN_USE,
    MG_PORTAL_STATIC_TABLES,
    MG_PORTAL_TABLES,
    MG_SEP,
    OUTPUT_DIR,
)

MG_INPUT = INPUT_DIR / "mg"


def _read_all_varchar(paths: list[Path], with_filename: bool = False) -> str:
    """A duckdb relation over the annual CSVs, every column forced to VARCHAR.

    `quote` and `escape` are pinned for the same reason as in `clean_mg.py`: most rows
    are unquoted, so the sniffer can conclude there is no quote character and then a
    legitimately quoted `objeto_contrato` containing a semicolon splits the row. Strict
    mode stays on so any other malformed row fails loudly instead of vanishing.
    """
    files = ", ".join(f"'{p}'" for p in paths)
    extra = ", filename=true" if with_filename else ""
    return (
        f"read_csv([{files}], delim='{MG_SEP}', header=true, all_varchar=true, "
        f"quote='\"', escape='\"', ignore_errors=false, union_by_name=true{extra})"
    )


def clean(con: duckdb.DuckDBPyConnection, stem: str, table: str) -> int:
    monthly = stem in MG_PORTAL_MONTHLY_TABLES
    static = stem in MG_PORTAL_STATIC_TABLES
    listed = stem in MG_PORTAL_LISTED_TABLES
    # Monthly files are `notas_jan22.csv`; annual ones `contratos2022.csv`.
    if listed:
        pattern = f"{stem}*.csv"
    elif static:
        pattern = f"{stem}.csv"
    elif monthly:
        pattern = f"{stem}[a-z][a-z][a-z][0-9][0-9].csv"
    else:
        pattern = f"{stem}[0-9][0-9][0-9][0-9].csv"
    srcs = sorted(MG_INPUT.glob(pattern))
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

    rel = _read_all_varchar(srcs, with_filename=monthly or listed)
    # Spreadsheet artefact columns, identified from the real header rather than from the
    # stale datapackage. An explicit column list is used instead of `* EXCLUDE (...)`
    # because EXCLUDE errors when the named column is absent, and only some year files
    # carry one.
    cols = [
        r[0] for r in con.execute(f"describe select * from {rel}").fetchall()
    ]
    keep = [
        c
        for c in cols
        if not re.fullmatch(r"unnamed_\d+|", c.strip()) and c != "filename"
    ]
    if dropped := [c for c in cols if c not in keep and c != "filename"]:
        print(f"    dropping artefact column(s): {', '.join(dropped)}")
    projection = ", ".join(f'"{c}"' for c in keep)
    # The item file carries no date column at all, so its period exists only in the
    # filename. Kept as provenance on both monthly tables rather than parsed here:
    # staging mirrors the source, and the dbt model derives ano/mes from it.
    if monthly or listed:
        projection += ", parse_filename(filename) as arquivo_origem"
    con.execute(
        f"COPY (SELECT {projection} FROM {rel}) TO '{dest / 'data.parquet'}' "
        "(FORMAT PARQUET, COMPRESSION SNAPPY)"
    )
    # pyrefly: ignore [unsupported-operation]
    n = con.execute(f"SELECT count(*) FROM {rel}").fetchone()[0]
    print(f"  {table}: {n:,} rows from {len(srcs)} file(s)")
    return n


def main(only: str | None = None) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    known = {
        **MG_PORTAL_TABLES,
        **MG_PORTAL_MONTHLY_TABLES,
        **MG_PORTAL_STATIC_TABLES,
        **MG_PORTAL_LISTED_TABLES,
    }
    stems = (
        [only]
        if only
        else [
            *MG_PORTAL_IN_USE,
            *MG_PORTAL_MONTHLY_IN_USE,
            *MG_PORTAL_STATIC_IN_USE,
            *MG_PORTAL_LISTED_IN_USE,
        ]
    )
    unknown = [s for s in stems if s not in known]
    if unknown:
        raise SystemExit(f"unknown stem(s) {unknown}; known: {sorted(known)}")
    con = duckdb.connect()
    total = 0
    for stem in stems:
        total += clean(con, stem, known[stem])
    print(f"  total: {total:,} rows")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--only",
        help=f"one stem of {sorted(MG_PORTAL_TABLES)} (default: those in use)",
    )
    args = ap.parse_args()
    main(only=args.only)
