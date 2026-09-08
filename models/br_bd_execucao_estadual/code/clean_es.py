"""Convert Espírito Santo source files to all-STRING staging parquet.

ES publishes three flat per-year families over SIGEFES (execution) and SIGA
(procurement): no dimensional model and no surrogate keys, because every dimension
already ships denormalised as a code+label pair on the row. So unlike MG there is
nothing to join here -- each source file mirrors 1:1 into a staging table, and the
mapping onto the canonical schema happens entirely in dbt.

Staging is all-STRING by house convention, and it must be all-STRING here
specifically: the recurring-pipeline upload path stringifies its header, so a typed
external table left behind by onboarding collides with the pipeline's later overwrite.
See .claude/rules/prefect-pipeline-conventions.md, "Staging parquet must be all-STRING".
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from constants import (
    ES_ALWAYS_FETCH_STEMS,
    ES_SEP,
    ES_TABLES,
    INPUT_DIR,
    OUTPUT_DIR,
)

ES_INPUT = INPUT_DIR / "es"

# Only `Despesas` carries its own exercise. Everything else in the procurement family
# has no year column at all -- `ItensLotes` is (item, lot, description, quantity,
# price) and nothing more -- so the exercise has to come from the file name, which for
# those families is reliable and agrees with the dates inside.
YEAR_COLUMN = {"Despesas": "Ano"}


def _read_all_varchar(path: Path) -> str:
    """A duckdb relation over one source CSV, every column forced to VARCHAR.

    `all_varchar` is what keeps staging faithful: no silent numeric coercion, no locale
    surprise on the BR decimal format (`88,0000`), and NULL stays NULL rather than
    becoming "nan", which safe_cast would not turn back into NULL downstream.

    duckdb strips the UTF-8 BOM itself, so `encoding='utf-8-sig'` is neither needed nor
    accepted. `quote` is pinned rather than sniffed: ES rows are mostly unquoted, and on
    such a file the sniffer concludes there is no quote character -- after which a
    legitimately quoted `HistoricoDocumento` containing a `;` blows the row apart.
    Strict mode stays on so any other malformed row fails loudly instead of vanishing.
    """
    return (
        f"read_csv('{path}', delim='{ES_SEP}', header=true, all_varchar=true, "
        f"quote='\"', escape='\"', ignore_errors=false)"
    )


def _year_expression(stem: str, year: str) -> tuple[str, str]:
    """The (projection, label) that gives a staging table its year column.

    The literal is QUOTED. duckdb types a bare `{year}` integer literal as INTEGER while
    `all_varchar` makes every other column VARCHAR, so an unquoted stamp is the one
    typed column in an otherwise all-STRING file. That exact mistake in clean_pe reached
    production and killed the first prod run:

        Parquet column 'ano' has type INT32 which does not match the target
        cpp_type STRING

    and four green dev runs could not catch it, because dev's external tables were built
    by upload.py (which infers types from the data) rather than by the flow's
    dump_header (which stringifies). See the memory note
    reference_onboarding_tables_mask_schema_mismatch.

    For the contratos family the file-name year is NOT an exercise -- it buckets a date
    that may be missing, and `Contratos-1753.csv` is SQL Server's datetime floor holding
    180 real contracts. Calling that `ano` would invite a downstream partition on a
    fiction, so it is named `ano_arquivo` and dbt derives the real year from the date
    columns.
    """
    label = "ano_arquivo" if stem in ES_ALWAYS_FETCH_STEMS else "ano"
    col = YEAR_COLUMN.get(stem)
    if col:
        # Normalised through INTEGER and back to VARCHAR, which strips a stray "2013.0"
        # so that safe_cast(ano as int64) still works downstream.
        return f"CAST(CAST({col} AS INTEGER) AS VARCHAR) AS {label}", label
    return f"'{year}' AS {label}", label


def clean_table(con: duckdb.DuckDBPyConnection, stem: str, table: str) -> int:
    # The year suffix is matched digit-by-digit rather than with a bare `*`, which also
    # keeps `ItensLotes` from swallowing `ItensLotesDisputas`: the literal `-` after the
    # stem is what separates them, and a `{stem}*` glob would file every bidder row into
    # the item table without a word. Same family as MG's dm_empenho_desp collision.
    srcs = sorted(ES_INPUT.glob(f"{stem}-[0-9][0-9][0-9][0-9].csv"))
    if not srcs:
        print(f"  SKIP {stem}: not downloaded")
        return 0

    dest = OUTPUT_DIR / table
    dest.mkdir(parents=True, exist_ok=True)
    # Clear previous output, including any leftover hive-partitioned `ano=YYYY/` layout
    # from an earlier run: a stale partition dir would be picked up by the uploader's
    # wildcard alongside the new flat files and double-count every row.
    for stale in dest.rglob("*.parquet"):
        stale.unlink()
    for sub in sorted(dest.glob("ano=*"), reverse=True):
        if sub.is_dir():
            sub.rmdir()

    total = 0
    for src in srcs:
        year = src.stem.rsplit("-", 1)[-1]
        out = dest / f"data_{year}.parquet"
        rel = _read_all_varchar(src)
        year_expr, label = _year_expression(stem, year)
        drop = f" EXCLUDE ({YEAR_COLUMN[stem]})" if stem in YEAR_COLUMN else ""
        con.execute(
            f"COPY (SELECT *{drop}, {year_expr} FROM {rel}) "
            f"TO '{out}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
        )
        n = con.execute(
            f"SELECT count(*) FROM read_parquet('{out}')"
        ).fetchone()[0]
        if n == 0:
            # An empty first partition makes dump_header infer INTEGER for every column
            # and poisons the staging schema for the whole table, so it is never worth
            # keeping. See reference_empty_parquet_partition_poisons_staging_schema.
            out.unlink()
            print(f"    {src.name}: 0 rows, parquet dropped")
            continue
        total += n
        print(f"    {src.name}: {n:,} rows", flush=True)

    files = sorted(dest.glob("data_*.parquet"))
    if not files:
        print(f"  {table}: no non-empty partitions")
        return 0

    # Assert the invariant rather than trusting that the quoting above was right. This
    # is cheap, and it is the check that would have caught the PE regression locally.
    types = con.execute(
        f"SELECT DISTINCT column_name, column_type "
        f"FROM (DESCRIBE SELECT * FROM read_parquet('{dest}/data_*.parquet'))"
        f"WHERE column_type <> 'VARCHAR'"
    ).fetchall()
    if types:
        raise SystemExit(
            f"{table}: staging parquet must be all-STRING, found {types}"
        )

    span = con.execute(
        f"SELECT min({label}), max({label}), count(DISTINCT {label}) "
        f"FROM read_parquet('{dest}/data_*.parquet')"
    ).fetchone()
    print(
        f"  {table}: {total:,} rows across {len(files)} files "
        f"-> {label} {span[0]}-{span[1]} ({span[2]} distinct)"
    )
    return total


def main(only: str | None = None) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    con.execute("PRAGMA memory_limit='6GB'")

    grand = 0
    for stem, table in ES_TABLES.items():
        if only and only != table:
            continue
        grand += clean_table(con, stem, table)
    print(f"\nES total: {grand:,} rows")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="build a single staging table")
    main(**vars(ap.parse_args()))
