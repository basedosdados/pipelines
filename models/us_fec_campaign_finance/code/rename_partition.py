"""One-off: rename the partition column and directory from `cycle` to `year`.

    python rename_partition.py

The partition was originally called `cycle`, since its value is the FEC's two-year
election cycle rather than a calendar year. The house convention for English datasets
is `year`, so it was renamed; the two-year meaning now lives in the column description
and observations instead of the column name.

Rewrites each parquet in place (streamed by row group, so peak memory is one group,
not one 2 GB file) and moves `cycle=YYYY/` to `year=YYYY/`. Idempotent: partitions
already named `year=` are skipped, so a failed run can just be re-run.

Delete this script once the rename has propagated everywhere — it exists only to avoid
re-downloading and re-cleaning 22 GB to change one column name.
"""

import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from pipelines.datasets.us_fec_campaign_finance import (
    utils as fec,
)

OLD, NEW = "cycle", "year"


def rewrite(src: Path, dest: Path) -> int:
    reader = pq.ParquetFile(src)
    schema = pa.schema(
        [
            pa.field(NEW if f.name == OLD else f.name, f.type)
            for f in reader.schema_arrow
        ]
    )
    dest.parent.mkdir(parents=True, exist_ok=True)
    rows = 0
    with pq.ParquetWriter(dest, schema, compression="snappy") as writer:
        for batch in reader.iter_batches(batch_size=500_000):
            writer.write_table(
                pa.Table.from_arrays(batch.columns, schema=schema)
            )
            rows += batch.num_rows
    return rows


def main() -> None:
    total = 0
    for table_dir in sorted(fec.OUTPUT.iterdir()):
        if not table_dir.is_dir():
            continue
        for part in sorted(table_dir.glob(f"{OLD}=*")):
            cycle = part.name.split("=", 1)[1]
            dest_dir = table_dir / f"{NEW}={cycle}"
            rows = 0
            for file in sorted(part.glob("*.parquet")):
                rows += rewrite(file, dest_dir / file.name)
                file.unlink()
            part.rmdir()
            total += rows
            print(
                f"  {table_dir.name:26s} {part.name} -> {dest_dir.name}  {rows:>12,}",
                flush=True,
            )
    print(f"\nrewrote {total:,} rows")


if __name__ == "__main__":
    main()
