"""Convert Rio de Janeiro's SEFAZ despesa series to all-STRING staging parquet.

One staging table, `rj_despesa`, from ten annual CSVs.

**RJ is a cumulative year-end SNAPSHOT, not a monthly series.** Each file contains
exactly one `Posição` -- `12/YYYY` for a closed exercise and the latest available month
for the open one -- so the grain is budget line x exercise, and the values are the
position reached by that month, not that month's movement:

    2016  12/2016  18,070 rows   R$ 60,832,044,102.88 empenhado
    2017  12/2017  17,224        R$ 67,965,548,697.57
    ...
    2024  12/2024  18,119        R$107,265,798,592.37
    2025  07/2025  15,428        R$ 53,029,325,651.79   (exercise still open)

That matters downstream and is why this module only stages the data:

  * It is NOT `despesa_mensal`'s grain. That table is month x budget line, and RJ has
    one month per exercise. Unioning them would put a cumulative annual figure beside
    monthly movements under the same column names.
  * It is NOT `despesa_anual`'s grain either. That table is creditor x budget line x
    year (São Paulo); RJ publishes no creditor at all.
  * It is certainly not `despesa`: no empenho document, no creditor, no date below the
    month.

Where RJ belongs is a schema decision, not a cleaning decision, so it is left to be
made deliberately. See SOURCE_LESSONS.md.

Two parsing notes, both measured:

  * **Five preamble lines precede the header** (ministry, secretariat, subsecretariat,
    "Transparência Fiscal", and the date range). A reader that assumes row 0 is the
    header treats the first data row as column names.
  * The files are plain latin-1 -- duckdb's strict latin-1 reader accepts all ten, so
    unlike RS and SC there are no C1 bytes and no transcode is needed. Dimension
    columns are quoted, value columns are not, and every row has exactly 41 fields.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from constants import (
    INPUT_DIR,
    OUTPUT_DIR,
    RJ_PREAMBLE_LINES,
    RJ_SEP,
    RJ_TABLE,
    normalise_column,
)

RJ_INPUT = INPUT_DIR / "rj"

EXPECTED_COLUMNS = 41


def _relation(path: Path) -> str:
    return (
        f"read_csv('{path}', skip={RJ_PREAMBLE_LINES}, header=true, "
        f"delim='{RJ_SEP}', quote='\"', all_varchar=true, encoding='latin-1', "
        f"ignore_errors=false, sample_size=-1)"
    )


def clean_year(
    con: duckdb.DuckDBPyConnection, source: Path, reference: list[str] | None
) -> tuple[int, list[str]]:
    rel = _relation(source)
    raw = [
        r[0] for r in con.execute(f"describe select * from {rel}").fetchall()
    ]
    if len(raw) != EXPECTED_COLUMNS:
        raise SystemExit(
            f"{source.name}: {len(raw)} columns, expected {EXPECTED_COLUMNS}. "
            f"The preamble length or the export layout has changed."
        )
    header = [normalise_column(c) for c in raw]
    if reference is not None and header != reference:
        missing = [c for c in reference if c not in header]
        extra = [c for c in header if c not in reference]
        raise SystemExit(
            f"{source.name}: header differs from the reference "
            f"(missing={missing}; extra={extra}). Resolve deliberately -- a silent "
            f"union would leave one layout's columns NULL for every row of the other."
        )

    # The exercise comes from `Posição` inside the file, never from the file name: the
    # open year's snapshot is a mid-year month, and the two must not disagree.
    dest_dir = OUTPUT_DIR / RJ_TABLE
    dest_dir.mkdir(parents=True, exist_ok=True)
    out_path = dest_dir / f"data_{source.stem.rsplit('_', 1)[-1]}.parquet"
    projection = ", ".join(
        f'"{r}" AS "{h}"' for r, h in zip(raw, header, strict=True)
    )
    con.execute(
        f"COPY (SELECT {projection} FROM {rel}) TO '{out_path}' "
        f"(FORMAT PARQUET, COMPRESSION SNAPPY)"
    )
    n = con.execute(
        f"SELECT count(*) FROM read_parquet('{out_path}')"
    ).fetchone()[0]
    if n == 0:
        # An empty first partition makes dump_header infer INTEGER for every column.
        out_path.unlink()
        print(f"    {source.name}: 0 rows, parquet dropped")
        return 0, header

    positions = [
        r[0]
        for r in con.execute(
            f"SELECT DISTINCT \"{header[0]}\" FROM read_parquet('{out_path}')"
        ).fetchall()
    ]
    if len(positions) != 1:
        raise SystemExit(
            f"{source.name}: {len(positions)} distinct Posição values ({positions}). "
            f"Every file so far carries exactly one; more than one would mean this is a "
            f"monthly series after all and the grain note in this module is wrong."
        )
    print(f"    {source.name}: {n:,} rows, posição {positions[0]}", flush=True)
    return n, header


def main(only_year: int | None = None) -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    sources = sorted(RJ_INPUT.glob(f"{RJ_TABLE}_*.csv"))
    if only_year is not None:
        sources = [p for p in sources if p.stem.endswith(str(only_year))]
    if not sources:
        print("  rj: no input files")
        return
    reference: list[str] | None = None
    total = 0
    for source in sources:
        n, header = clean_year(con, source, reference)
        reference = reference or header
        total += n
    print(f"{RJ_TABLE:<16} {total:>12,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int)
    args = parser.parse_args()
    main(only_year=args.year)
