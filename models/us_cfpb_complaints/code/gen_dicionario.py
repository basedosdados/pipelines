"""Build the `dicionario` table from the cleaned complaint parquet.

The CFPB publishes its categorical fields as readable English labels rather than
codes, so `valor` reproduces `chave`. What the dictionary adds is
`cobertura_temporal`: the years in which each value actually appears. The complaint
form's product/issue taxonomy was revised in April 2017 and again in August 2023 and
values are preserved exactly as published, never remapped, so the same concept shows
up under several labels with disjoint coverage — for example:

    product  Credit reporting                                              2012(1)2017
    product  Credit reporting, credit repair services, or other personal…  2017(1)2023
    product  Credit reporting or other personal consumer reports           2023(1)2026

Reading the parquet rather than the raw CSV keeps the dictionary consistent with
what is actually published to BigQuery.
"""

import argparse
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from common import COMPLAINT, DICIONARIO, DICT_COLUMNS, OUTPUT, load_cols


def build(output_dir: Path) -> int:
    """Write `<output_dir>/dicionario/data.parquet`; returns the row count."""
    parts = sorted((output_dir / COMPLAINT).glob("year=*/data.parquet"))
    if not parts:
        raise SystemExit(
            f"no complaint parquet under {output_dir / COMPLAINT}"
        )

    # (column, value) -> [min_year, max_year]
    span: dict[tuple[str, str], list[int]] = defaultdict(lambda: [9999, 0])
    for p in parts:
        year = int(p.parent.name.split("=", 1)[1])
        # ParquetFile.read, not pq.read_table: the latter infers a hive dataset from
        # the `year=<YYYY>` directory and then refuses to merge that inferred
        # dictionary<int32> `year` with the STRING `year` inside the file. BigQuery
        # reads the in-file column, which is the one that matters.
        tbl = pq.ParquetFile(p).read(columns=DICT_COLUMNS)
        for col in DICT_COLUMNS:
            for v in tbl.column(col).unique().to_pylist():
                if v is None or v == "":
                    continue
                s = span[(col, v)]
                s[0] = min(s[0], year)
                s[1] = max(s[1], year)
        print(f"  read {p.parent.name}", flush=True)

    rows = []
    for (col, value), (y0, y1) in sorted(span.items()):
        rows.append(
            {
                "id_tabela": COMPLAINT,
                "nome_coluna": col,
                "chave": value,
                # Data Basis temporal-coverage notation: START(INTERVAL)END
                "cobertura_temporal": f"{y0}(1){y1}",
                "valor": value,
            }
        )

    order = [c.name for c in load_cols(DICIONARIO)]
    schema = pa.schema([pa.field(n, pa.string()) for n in order])
    table = pa.Table.from_arrays(
        [pa.array([r[n] for r in rows], type=pa.string()) for n in order],
        schema=schema,
    )
    ddir = output_dir / DICIONARIO
    ddir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, ddir / "data.parquet", compression="snappy")

    print(f"\nwrote {ddir / 'data.parquet'}  ({len(rows)} rows)")
    per_col: dict[str, int] = defaultdict(int)
    for r in rows:
        per_col[r["nome_coluna"]] += 1
    for col in DICT_COLUMNS:
        print(f"  {col:32s} {per_col[col]:>4} values")
    return len(rows)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--output", type=Path, default=OUTPUT)
    build(ap.parse_args().output)
