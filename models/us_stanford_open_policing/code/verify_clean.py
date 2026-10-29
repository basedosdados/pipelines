"""Check the cleaned Parquet against the source CSVs, independently of DuckDB.

For each jurisdiction, re-reads the zipped CSV with Python's ``csv`` module and
compares, column by column, the number of non-null values (not ``NA``, not
empty) with the cleaned Parquet. Any difference is a lost or invented value.

Usage:
    python models/us_stanford_open_policing/code/verify_clean.py [slug ...]
"""

import csv
import io
import json
import sys
import zipfile

import duckdb

try:
    from models.us_stanford_open_policing.code import constants
except ModuleNotFoundError:  # run as a script
    import constants  # type: ignore[no-redef]

csv.field_size_limit(sys.maxsize)


def source_counts(slug: str) -> tuple[int, dict[str, int]]:
    with zipfile.ZipFile(constants.source_file(slug)) as z:
        (member,) = [n for n in z.namelist() if n.endswith(".csv")]
        with z.open(member) as raw:
            reader = csv.reader(
                io.TextIOWrapper(raw, encoding="utf-8", newline="")
            )
            header = next(reader)
            keep = [
                (i, h)
                for i, h in enumerate(header)
                if not h.startswith("raw_")
            ]
            counts = {h: 0 for _, h in keep}
            rows = 0
            for rec in reader:
                rows += 1
                for i, h in keep:
                    # NA is the null marker only verbatim; " NA" is text
                    if rec[i] != "NA" and rec[i].strip(" \t\n\x0b\x0c\r"):
                        counts[h] += 1
    return rows, counts


def parquet_counts(slug: str, cols: list[str]) -> tuple[int, dict[str, int]]:
    exprs = ", ".join(f'count("{c}")' for c in cols)
    glob = constants.OUTPUT_DIR / "stop" / "**" / f"{slug}_*.parquet"
    row = duckdb.sql(
        f"select count(*), {exprs} from read_parquet('{glob}', hive_partitioning = false)"
    ).fetchone()
    return row[0], dict(zip(cols, row[1:], strict=True))


def main() -> None:
    slugs = sys.argv[1:] or sorted(
        p.stem for p in (constants.OUTPUT_DIR / "_manifest").glob("*.json")
    )
    bad = []
    for slug in slugs:
        rows, src = source_counts(slug)
        prows, out = parquet_counts(slug, list(src))
        manifest = json.loads(
            (constants.OUTPUT_DIR / "_manifest" / f"{slug}.json").read_text()
        )
        diffs = {c: (src[c], out[c]) for c in src if src[c] != out[c]}
        ok = rows == prows == manifest["rows"] and not diffs
        print(
            f"{slug}: rows src={rows:,} parquet={prows:,} {'OK' if ok else 'MISMATCH'} {diffs or ''}",
            flush=True,
        )
        if not ok:
            bad.append(slug)
    if bad:
        sys.exit(f"mismatch: {bad}")


if __name__ == "__main__":
    main()
