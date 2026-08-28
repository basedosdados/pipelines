"""Convert Bahia source archives to all-STRING staging parquet.

BA ships one CSV per database view inside a few ZIPs. The views are already denormalised --
every classification carries its code and its label side by side -- so, as with MG, this
step mirrors them faithfully and the mapping onto the canonical schema happens in dbt.

The CSVs are read straight out of the ZIP rather than extracted first: VW_PAINEL_DESPESA
alone is 2.37 GB unpacked, and there is no reason to land it twice.
"""

from __future__ import annotations

import argparse
import csv
import re
import shutil
import sys
import unicodedata
import zipfile
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from constants import BA_TABLES, INPUT_DIR, OUTPUT_DIR

BA_INPUT = INPUT_DIR / "ba"
EXTRACT_DIR = BA_INPUT / "extracted"

# Views whose free-text columns contain unescaped double quotes, so the published CSV is
# not parseable as published. See repair().
NEEDS_REPAIR = {"VW_PROC_AQUISICAO_ITEM", "VW_PROC_AQUISICAO_FORNEC"}

# Which view carries the exercise, so the parquet can be split per year. BA's procurement
# item and supplier views are keyed by process rather than by exercise, and are written as
# a single file.
YEAR_COLUMN = {
    "VW_PAINEL_DESPESA": "ano_exercicio",
    "VW_PROCESSO_SEI": "ano_exercicio",
    # "Ano da Aquisição" after normalise()
    "VW_PROC_AQUISICAO_LIC_REQ": "ano_da_aquisicao",
}


def _header(path: Path) -> list[str]:
    """Column names from the first line, BOM stripped and quotes removed."""
    with open(path, encoding="utf-8-sig", newline="") as fh:
        line = fh.readline().rstrip("\r\n")
    return [c.strip().strip('"') for c in line.split(";")]


def normalise(name: str) -> str:
    """A BigQuery-legal column name.

    BA's procurement views use human column headings -- "N° da Licitação", "Processo de
    Aquisição", "Nome do Item Completo" -- with spaces, accents and a degree sign.
    BigQuery requires column names to match [A-Za-z_][A-Za-z_0-9]*, so loading the parquet
    as published would fail. Accents are folded to ASCII, everything else becomes an
    underscore, and the result is lowercased, so `dm_processo` and `VW_PROC_...` end up
    with names in the same style as MG's.
    """
    folded = unicodedata.normalize("NFKD", name)
    ascii_only = folded.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^0-9a-zA-Z]+", "_", ascii_only).strip("_").lower()
    slug = re.sub(r"_+", "_", slug)
    return slug or "coluna"


def _relation(path: Path, header: list[str] | None = None) -> str:
    """A duckdb relation over one BA view, dialect declared rather than sniffed.

    BA quotes every field and separates with ';'. Types are forced to VARCHAR so the BR
    decimal format ("2643000,00") reaches dbt unmangled.

    Auto-detection is switched OFF and the columns are declared explicitly. Item
    descriptions in VW_PROC_AQUISICAO_ITEM contain **embedded newlines inside quoted
    fields**, so a line-sampling sniffer sees rows with 1, 3, 19, 22, 27, 32... fields and
    gives up with "not possible to automatically detect the CSV parsing dialect". Declaring
    the schema removes the guesswork; the parser then honours the quotes and reassembles
    the multi-line values correctly.

    Views listed in NEEDS_REPAIR are read from the output of repair() rather than from the
    published file, and carry no header row of their own.
    """
    repaired = path.stem.endswith("_repaired")
    columns = ", ".join(
        f"'{normalise(c)}': 'VARCHAR'" for c in (header or _header(path))
    )
    return (
        f"read_csv('{path}', delim=';', header={str(not repaired).lower()}, "
        f"auto_detect=false, quote='\"', escape='\"', ignore_errors=false, "
        f"columns={{{columns}}})"
    )


def repair(src: Path, n_cols: int) -> Path:
    """Rewrite a BA view as valid CSV, recovering rows its export corrupted.

    BA does not escape the double quote inside quoted fields, and item descriptions use it
    freely -- both as a quotation mark and as an inches marker:

        ..."Suspensao de celulas ... do grupo "O""...        -> parser stops
        ..."Pandeiro aro 10 ", pele couro animal"...         -> field ends early, 20 of 22

    No parser setting fixes the second case: once the quote closes early the row genuinely
    has the wrong number of fields, so `strict_mode=false` mis-parses it rather than
    rejecting it. The file has to be repaired.

    What makes that tractable is that BA's *separators* are unambiguous even though its
    quoting is not. A field boundary is the three characters `";"`, and a record boundary is
    a quote followed by a newline. Neither sequence can arise from a stray quote inside a
    description, so splitting on them recovers the intended fields exactly -- including the
    two examples above, and including descriptions containing embedded newlines.

    Records are accumulated until they hold the expected number of separators, so a field
    spanning several physical lines is reassembled rather than truncated. Anything that
    still does not reach `n_cols` is reported and skipped rather than silently padded.
    """
    dest = src.with_name(src.stem + "_repaired.csv")
    if dest.exists() and dest.stat().st_size > 0:
        return dest

    sep = '";"'
    written = skipped = 0
    with (
        open(src, encoding="utf-8-sig", newline="") as fh,
        open(dest, "w", encoding="utf-8", newline="") as out,
    ):
        writer = csv.writer(out, delimiter=";", quoting=csv.QUOTE_ALL)
        buffer = ""
        for line in fh:
            buffer += line
            if buffer.count(sep) < n_cols - 1:
                continue  # a field contains a newline; keep reading
            record = buffer.rstrip("\r\n")
            fields = record.split(sep)
            if len(fields) == n_cols:
                fields[0] = fields[0].removeprefix('"')
                fields[-1] = fields[-1].removesuffix('"')
                writer.writerow(fields)
                written += 1
            else:
                skipped += 1
            buffer = ""
        if buffer.strip():
            skipped += 1

    print(
        f"    repaired {src.name}: {written:,} rows written, {skipped:,} skipped"
    )
    if skipped:
        # Loud, not fatal: a handful of unrecoverable rows out of millions is tolerable,
        # but the count has to be visible so it can never quietly become thousands.
        print(
            f"    WARNING: {skipped:,} rows could not be split into {n_cols} fields"
        )
    return dest


def extract(view: str) -> Path | None:
    """Unpack one view from whichever archive holds it, if not already unpacked."""
    EXTRACT_DIR.mkdir(parents=True, exist_ok=True)
    dest = EXTRACT_DIR / f"{view}.csv"
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    for archive in sorted(BA_INPUT.glob("*.zip")):
        with zipfile.ZipFile(archive) as zf:
            for member in zf.namelist():
                if Path(member).stem.upper() != view.upper():
                    continue
                with zf.open(member) as src, open(dest, "wb") as out:
                    shutil.copyfileobj(src, out, length=1 << 20)
                return dest
    print(f"  SKIP {view}: not found in any archive")
    return None


def clean(con: duckdb.DuckDBPyConnection, view: str, table: str) -> int:
    src = extract(view)
    if src is None:
        return 0
    dest = OUTPUT_DIR / table
    dest.mkdir(parents=True, exist_ok=True)
    for stale in dest.glob("*.parquet"):
        stale.unlink()

    # Views whose descriptions carry unescaped quotes are repaired first; the rest are
    # already valid CSV and are read as published.
    header = _header(src)
    if view in NEEDS_REPAIR:
        src = repair(src, len(header))
    rel = _relation(src, header)
    year_col = YEAR_COLUMN.get(view)
    if year_col is None:
        con.execute(
            f"COPY (SELECT * FROM {rel}) TO '{dest / 'data.parquet'}' "
            "(FORMAT PARQUET, COMPRESSION SNAPPY)"
        )
        total = con.execute(f"SELECT count(*) FROM {rel}").fetchone()[0]
        print(f"  {table}: {total:,} rows")
        return total

    # Split per exercise so no single parquet is enormous, and so the uploader's 0-row
    # header file has small siblings. `ano` is normalised to VARCHAR for the same reason
    # as in clean_mg.py: staging is all-STRING, and one typed column is enough to break a
    # later pipeline overwrite of the same prefix.
    years = [
        r[0]
        for r in con.execute(
            f"SELECT DISTINCT CAST({year_col} AS INTEGER) y FROM {rel} "
            f"WHERE {year_col} IS NOT NULL ORDER BY y"
        ).fetchall()
    ]
    total = 0
    for year in years:
        out = dest / f"data_{year}.parquet"
        con.execute(
            f"COPY (SELECT * EXCLUDE ({year_col}), "
            f"       CAST(CAST({year_col} AS INTEGER) AS VARCHAR) AS ano "
            f"      FROM {rel} WHERE CAST({year_col} AS INTEGER) = {year}) "
            f"TO '{out}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
        )
        total += con.execute(
            f"SELECT count(*) FROM read_parquet('{out}')"
        ).fetchone()[0]
    print(
        f"  {table}: {total:,} rows -> {len(years)} year files ({years[0]}-{years[-1]})"
    )
    return total


def verify_item_parse(con: duckdb.DuckDBPyConnection, table: str) -> None:
    """Guard against `strict_mode=false` having silently shifted fields.

    If an unescaped quote splits a field, every later value on that row moves one column
    left and text lands in the numeric columns. Checking that the value columns still parse
    as numbers catches that; a clean parse leaves them ~100% numeric.
    """
    path = OUTPUT_DIR / table / "*.parquet"
    numeric = ["quantidade", "val_item_estimado", "val_item_total_estimado"]
    checks = ", ".join(
        f"countif(try_cast(replace({c}, ',', '.') AS DOUBLE) IS NOT NULL) "
        f"/ nullif(countif({c} IS NOT NULL), 0) AS {c}"
        for c in numeric
    )
    row = con.execute(
        f"SELECT {checks} FROM read_parquet('{path}')"
    ).fetchone()
    for name, rate in zip(numeric, row, strict=True):
        status = (
            "OK" if rate is None or rate > 0.999 else "SUSPECT FIELD SHIFT"
        )
        print(
            f"    {name}: {0 if rate is None else rate:.4%} numeric  {status}"
        )
        if rate is not None and rate <= 0.999:
            raise ValueError(
                f"{table}.{name} only {rate:.4%} numeric -- lenient CSV parsing has "
                "probably misaligned fields"
            )


def main(only: str | None = None) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    con.execute("PRAGMA memory_limit='6GB'")
    for view, table in BA_TABLES.items():
        if only and only != table:
            continue
        clean(con, view, table)
        if view == "VW_PROC_AQUISICAO_ITEM":
            verify_item_parse(con, table)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="build a single staging table")
    main(**vars(ap.parse_args()))
