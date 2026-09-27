"""Build every au_treasury_budget table and write it as staging Parquet.

Runs the three extractors, derives the dictionary from what they actually
produced, and writes all four tables as all-STRING Snappy Parquet partitioned by
``year``.

Staging is all-STRING by house convention: the dbt model ``safe_cast``s every
column to its architecture type, and ``pipelines.utils.gcs.dump_header``
stringifies the one-row header BigQuery infers the staging schema from. Typed
Parquet against that STRING schema is rejected on read, so values pass through
the architecture's real types first -- making ``year`` serialize as ``"1970"``
rather than ``"1970.0"`` -- and are only then cast to string via arrow. Never
``astype(str)``: it renders a NULL as the literal ``"nan"``, which ``safe_cast``
will not turn back into NULL, and these tables are mostly NULL by design, since a
measure published only in dollars has no percentage-of-GDP value.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import pathlib
import shutil
import sys

import clean_aggregate
import clean_igr_projection
import clean_payment_growth
import dictionary
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

CODE = pathlib.Path(__file__).resolve().parent
ARCHITECTURE = CODE / "architecture"

DATA_ROOT = pathlib.Path(
    os.environ.get(
        "AU_TREASURY_BUDGET_DATA",
        pathlib.Path.home() / "Downloads" / "au_treasury_budget_data",
    )
)

ARROW_TYPES = {
    "STRING": pa.string(),
    "INT64": pa.int64(),
    "FLOAT64": pa.float64(),
}

#: Tables partitioned by year. The dictionary has no temporal dimension.
PARTITIONED = ("aggregate", "payment_growth", "igr_projection")


def read_architecture(table: str) -> list[dict]:
    with (ARCHITECTURE / f"{table}.csv").open() as handle:
        return list(csv.DictReader(handle))


def write_table(rows: list[dict], table: str, output_dir: pathlib.Path) -> int:
    """Write one table to all-STRING Parquet, partitioned by year where it has one."""
    architecture = read_architecture(table)
    order = [column["name"] for column in architecture]
    typed_schema = pa.schema(
        [
            pa.field(c["name"], ARROW_TYPES[c["bigquery_type"]])
            for c in architecture
        ]
    )
    string_schema = pa.schema([pa.field(name, pa.string()) for name in order])

    frame = pd.DataFrame(rows)
    missing = set(order) - set(frame.columns)
    if missing:
        raise ValueError(
            f"{table}: the extractor produced no {sorted(missing)}, but the "
            "architecture declares them. The two must agree -- the architecture "
            "is the source of truth."
        )
    extra = set(frame.columns) - set(order)
    if extra:
        raise ValueError(
            f"{table}: the extractor produced {sorted(extra)}, which the "
            "architecture does not declare. Add them there first."
        )
    frame = frame[order]

    table_dir = output_dir / table
    if table_dir.exists():
        shutil.rmtree(table_dir)

    def write_group(group: pd.DataFrame, directory: pathlib.Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        arrow = pa.Table.from_pandas(
            group, schema=typed_schema, preserve_index=False
        )
        arrow = arrow.cast(string_schema)
        pq.write_table(arrow, directory / "data.parquet", compression="snappy")

    if table in PARTITIONED:
        for year, group in frame.groupby("year", sort=True):
            write_group(group, table_dir / f"year={int(year)}")
    else:
        write_group(frame, table_dir)
    return len(frame)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=str(DATA_ROOT / "output"))
    args = parser.parse_args()
    output_dir = pathlib.Path(args.out)

    print("aggregate")
    aggregate_rows: list[dict] = []
    for release in clean_aggregate.releases_with_historical():
        aggregate_rows.extend(clean_aggregate.extract_release(release))
    failures = clean_aggregate.check_identities(aggregate_rows)
    if failures:
        for failure in failures[:20]:
            print("  ", failure)
        raise SystemExit("aggregate: accounting identities failed")

    print("\npayment_growth")
    growth_rows: list[dict] = []
    for release_id in clean_payment_growth.CHART_LOCATIONS:
        growth_rows.extend(clean_payment_growth.extract_release(release_id))
    failures = clean_payment_growth.check_cross_release_agreement(growth_rows)
    if failures:
        for failure in failures:
            print("  ", failure)
        raise SystemExit("payment_growth: vintages disagree")

    print("\nigr_projection")
    summary = clean_igr_projection.extract_summary()
    sensitivity = clean_igr_projection.extract_sensitivity()
    failures = clean_igr_projection.check_baseline_agreement(
        summary, sensitivity
    )
    if failures:
        for failure in failures[:20]:
            print("  ", failure)
        raise SystemExit("igr_projection: A1 and A4 baselines disagree")
    igr_rows = clean_igr_projection.merge(summary, sensitivity)

    print("\ndicionario")
    dictionary_rows = dictionary.build(
        aggregate=aggregate_rows,
        payment_growth=growth_rows,
        igr_projection=igr_rows,
    )

    print("\nwriting parquet")
    counts = {}
    for table, rows in (
        ("aggregate", aggregate_rows),
        ("payment_growth", growth_rows),
        ("igr_projection", igr_rows),
        ("dicionario", dictionary_rows),
    ):
        counts[table] = write_table(rows, table, output_dir)
        print(f"  {table:16s} {counts[table]:6,d} rows")

    (DATA_ROOT / "row_counts.json").write_text(json.dumps(counts, indent=2))
    print(f"\ntotal rows: {sum(counts.values()):,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
