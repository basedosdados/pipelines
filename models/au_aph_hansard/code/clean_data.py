"""Parse staged Hansard XML into partitioned, all-STRING Parquet.

Staging is all-STRING by house convention: the dbt model ``safe_cast``s each
column to its architecture type, so the Parquet schema carries column order,
not types. Values reach here as strings already, so no numeric round-trip can
turn 1959 into "1959.0" or a NULL into "nan".

Usage:
    python models/au_aph_hansard/code/clean_data.py [--year 1901] [--workers 4]
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.au_aph_hansard.utils import (
    parse_sitting_day,
)

DATA_ROOT = Path(
    os.environ.get(
        "AU_APH_HANSARD_DATA",
        Path.home() / "Downloads" / "au_aph_hansard_data",
    )
)
INPUT_DIR = DATA_ROOT / "input"
OUTPUT_DIR = DATA_ROOT / "output"

ARCHITECTURE = Path(__file__).resolve().parent / "architecture"


def architecture_columns(table: str) -> list[str]:
    """Column order for a table, read from its architecture CSV."""
    with (ARCHITECTURE / f"{table}.csv").open() as handle:
        return [row["name"] for row in csv.DictReader(handle)]


def write_partition(
    rows: list[dict],
    table: str,
    year: str,
    columns: list[str],
    part: str = "data",
) -> int:
    """Write one year of one table as an all-STRING Parquet partition.

    ``part`` names the file within the partition directory. A hive partition may
    hold several files, so naming the file after the source directory means a
    misfiled transcript writing into another year's partition adds a file rather
    than clobbering that year's data.
    """
    if not rows:
        return 0
    # `year` is the hive partition key, so it is not repeated inside the file.
    payload = [column for column in columns if column != "year"]
    schema = pa.schema([pa.field(name, pa.string()) for name in payload])
    table_arrow = pa.Table.from_pydict(
        {
            name: pa.array([row.get(name) for row in rows], type=pa.string())
            for name in payload
        },
        schema=schema,
    )
    destination = OUTPUT_DIR / table / f"year={year}"
    destination.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        table_arrow, destination / f"{part}.parquet", compression="snappy"
    )
    return len(rows)


def clean(
    only_year: str | None = None,
    min_year: int | None = None,
    max_year: int | None = None,
) -> dict:
    """Parse every staged transcript, one year at a time.

    Year-at-a-time rather than all-at-once: the full corpus is roughly two
    million utterances carrying their whole text, which does not want to sit in
    memory in one dict.
    """
    speech_columns = architecture_columns("speech")
    day_columns = architecture_columns("sitting_day")
    stats = Counter()

    by_year: dict[str, list[Path]] = defaultdict(list)
    for path in sorted(INPUT_DIR.rglob("*.xml")):
        year_dir = path.parent.name
        if only_year and year_dir != only_year:
            continue
        if year_dir.isdigit():
            if min_year and int(year_dir) < min_year:
                continue
            if max_year and int(year_dir) > max_year:
                continue
        by_year[year_dir].append(path)

    total = sum(len(v) for v in by_year.values())
    print(
        f"parsing {total:,} transcripts across {len(by_year)} years ...",
        flush=True,
    )

    for year_dir in sorted(by_year):
        speeches: list[dict] = []
        days: list[dict] = []
        seen: set[tuple[str, str]] = set()

        for path in by_year[year_dir]:
            house = path.relative_to(INPUT_DIR).parts[0]
            try:
                day, rows = parse_sitting_day(
                    path.read_bytes(), house, path.name
                )
            except Exception as exc:
                stats[f"unparsed_{type(exc).__name__}"] += 1
                continue

            key = (day.get("date") or path.name, day["chamber"])
            if key in seen:
                # The same sitting day is occasionally published twice, as a
                # proof and then an official transcript. Keep the first.
                stats["duplicate_day"] += 1
                continue
            seen.add(key)

            if not day.get("date"):
                stats["missing_date"] += 1
            if not rows:
                stats["empty_transcript"] += 1

            year = day.get("year") or year_dir
            day["year"] = year
            days.append(day)
            for row in rows:
                row["year"] = row.get("year") or year
                speeches.append(row)
            stats["days"] += 1
            stats["speeches"] += len(rows)

        # Partition on the year parsed out of the transcript, not the directory
        # it was filed under, so a misfiled transcript cannot land in a
        # partition whose key contradicts its own date column.
        for label, rows, table, columns in (
            ("speech", speeches, "speech", speech_columns),
            ("sitting_day", days, "sitting_day", day_columns),
        ):
            grouped: dict[str, list[dict]] = defaultdict(list)
            for row in rows:
                grouped[row.get("year") or year_dir].append(row)
            if len(grouped) > 1:
                stats["year_mismatch"] += 1
                print(
                    f"  ! {year_dir}/{label} spans years {sorted(grouped)}",
                    flush=True,
                )
            for year, subset in grouped.items():
                write_partition(
                    subset, table, year, columns, part=f"part-{year_dir}"
                )
        print(
            f"  {year_dir}: {len(days):>4} days, {len(speeches):>7,} utterances",
            flush=True,
        )

    write_dictionary()
    return dict(stats)


def write_dictionary() -> None:
    """Emit the dicionario table for the coded columns."""
    columns = architecture_columns("dicionario")
    entries = [
        ("speech", "talk_type", "speech", "Opening turn of a speech"),
        (
            "speech",
            "talk_type",
            "interjection",
            "Interruption by another member",
        ),
        (
            "speech",
            "talk_type",
            "continuation",
            "Original speaker resuming after an interjection",
        ),
        (
            "speech",
            "in_government",
            "0",
            "Speaker was not in the governing party",
        ),
        ("speech", "in_government", "1", "Speaker was in the governing party"),
        ("speech", "first_speech", "0", "Not the speaker's first speech"),
        (
            "speech",
            "first_speech",
            "1",
            "The speaker's first speech to the chamber",
        ),
        ("sitting_day", "is_proof", "0", "Official record"),
        (
            "sitting_day",
            "is_proof",
            "1",
            "Proof transcript, subject to revision",
        ),
    ]
    rows = [
        {
            "id_tabela": table,
            "nome_coluna": column,
            "chave": key,
            "cobertura_temporal": None,
            "valor": value,
        }
        for table, column, key, value in entries
    ]
    schema = pa.schema([pa.field(name, pa.string()) for name in columns])
    arrow = pa.Table.from_pydict(
        {
            name: pa.array([row.get(name) for row in rows], type=pa.string())
            for name in columns
        },
        schema=schema,
    )
    destination = OUTPUT_DIR / "dicionario"
    destination.mkdir(parents=True, exist_ok=True)
    pq.write_table(arrow, destination / "data.parquet", compression="snappy")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", default=None)
    parser.add_argument("--min-year", type=int, default=None)
    parser.add_argument("--max-year", type=int, default=None)
    args = parser.parse_args()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    stats = clean(args.year, args.min_year, args.max_year)
    print(f"\ndone: {stats}")


if __name__ == "__main__":
    main()
