"""Measure each column's non-null proportion over the cleaned parquet.

Two uses. It is the evidence behind the ``ignore_values`` lists in schema.yml --
``not_null_proportion_multiple_columns`` fails a table if any column is under 5%
populated, and several SDMX attributes are declared by the DSD but never filled
by the OECD for a given cube. Recording the measurement here means those
exemptions are a fact about the data rather than a hand-written list that drifts.

It also catches the opposite failure: a column that *should* carry values coming
out empty because the cleaner dropped it. A column at exactly 0.0 is worth
checking against the raw CSV before exempting it -- for the student cube the
seven zero columns are 0% populated in the source too, verified directly.

Writes ``measured.json``, which ``gen_dbt.py`` reads.

Run: ``python verify_parquet.py``   (after clean.py)
"""

import json

import pyarrow.compute as pc
import pyarrow.parquet as pq
from common import CODE_DIR, OUTPUT
from tables import TABLES

THRESHOLD = 0.05


def measure(slug):
    """{column: non-null proportion} across a table's cleaned parquet."""
    files = sorted((OUTPUT / slug).rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"{slug} has no cleaned parquet")
    total = 0
    filled = {}
    for path in files:
        table = pq.ParquetFile(path).read()
        total += table.num_rows
        for name in table.schema.names:
            col = table.column(name)
            nonnull = col.length() - col.null_count
            # Staging is all-STRING, so an empty string is as absent as a null.
            blank = pc.sum(pc.equal(col, "")).as_py() or 0
            filled[name] = filled.get(name, 0) + nonnull - blank
    return {
        name: (n / total if total else 0.0) for name, n in filled.items()
    }, total


def main():
    out = {}
    for slug in TABLES:
        props, total = measure(slug)
        sparse = sorted(c for c, p in props.items() if p < THRESHOLD)
        out[slug] = {
            "rows": total,
            "proportions": {c: round(p, 6) for c, p in sorted(props.items())},
            "sparse": sparse,
        }
        empty = [c for c in sparse if props[c] == 0.0]
        print(
            f"  {slug:24s} {total:10,d} rows | {len(sparse)} sparse, {len(empty)} entirely empty"
        )
        for c in sparse:
            print(f"       {c:30s} {props[c] * 100:6.2f}% populated")
    (CODE_DIR / "measured.json").write_text(json.dumps(out, indent=1))
    print(f"\nwrote {CODE_DIR / 'measured.json'}")


if __name__ == "__main__":
    main()
