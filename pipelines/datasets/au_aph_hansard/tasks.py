"""Prefect tasks for au_aph_hansard. Thin wrappers over the pure functions."""

from __future__ import annotations

import csv
import shutil
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path
from threading import Lock

import pyarrow as pa
import pyarrow.parquet as pq
from prefect import task

from pipelines.datasets.au_aph_hansard.constants import constants
from pipelines.datasets.au_aph_hansard.utils import (
    ProbeError,
    build_download_url,
    daterange,
    find_sitting_day_xml,
    http_get,
    is_hansard_xml,
    parse_sitting_day,
)


def _architecture_columns(table: str) -> list[str]:
    path = Path(constants.ARCHITECTURE_DIR.value) / f"{table}.csv"
    with path.open() as handle:
        return [row["name"] for row in csv.DictReader(handle)]


def _years_to_rebuild(today: date) -> list[int]:
    """Years whose partitions this run rebuilds.

    Always the current year. Also the previous one until the end of February,
    because a sitting day published in proof form late in a year is replaced by
    the official transcript weeks later, and January runs would otherwise never
    pick that correction up.
    """
    years = [today.year]
    if today.month <= 2:
        years.append(today.year - 1)
    return sorted(years)


@task(log_prints=True, retries=2, retry_delay_seconds=120)
def download_hansard(work_dir: str, today: date | None = None) -> dict:
    """Probe ParlInfo across the rebuild window and download each transcript.

    Rebuilding whole years rather than appending single days keeps the run
    idempotent: re-running cannot double-count a sitting day, and a proof
    transcript later replaced by the official one is corrected in place.
    """
    today = today or date.today()
    input_dir = Path(work_dir) / "input"
    input_dir.mkdir(parents=True, exist_ok=True)

    targets: list[tuple[str, date]] = []
    for year in _years_to_rebuild(today):
        first = date(year, 1, 1)
        last = min(date(year, 12, 31), today)
        targets += [
            (house, day)
            for house in constants.CHAMBERS.value
            for day in daterange(first, last)
        ]

    print(
        f"probing {len(targets):,} chamber-days across {_years_to_rebuild(today)}"
    )

    outcomes: Counter[str] = Counter()
    failures: list[str] = []
    lock = Lock()

    def grab(item: tuple[str, date]) -> str | None:
        house, day = item
        try:
            relative = find_sitting_day_xml(house, day)
        except ProbeError as exc:
            with lock:
                outcomes["probe_failed"] += 1
                if len(failures) < 5:
                    failures.append(str(exc))
            return None
        if relative is None:
            with lock:
                outcomes["did_not_sit"] += 1
            return None
        try:
            payload = http_get(build_download_url(relative))
        except Exception as exc:
            with lock:
                outcomes["download_failed"] += 1
                if len(failures) < 5:
                    failures.append(
                        f"{house} {day}: download {type(exc).__name__}"
                    )
            return None
        if not is_hansard_xml(payload):
            # ParlInfo serves a "Missing File" HTML page with HTTP 200.
            with lock:
                outcomes["not_a_transcript"] += 1
            return None
        dest = input_dir / house / str(day.year) / f"{day.isoformat()}.xml"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(payload)
        with lock:
            outcomes["downloaded"] += 1
        return str(dest)

    with ThreadPoolExecutor(max_workers=6) as pool:
        found = [path for path in pool.map(grab, targets) if path]

    print(f"probe outcomes: {dict(outcomes)}")
    for line in failures:
        print(f"  ! {line}")

    # A refused probe is not an answer. Failing here is the whole point: the
    # previous version mapped every failure to "did not sit", so a run in which
    # ParlInfo refused all 490 probes reported Completed having ingested
    # nothing, which is indistinguishable from a quiet parliamentary recess.
    broken = outcomes["probe_failed"] + outcomes["download_failed"]
    if broken:
        raise RuntimeError(
            f"{broken} of {len(targets)} probes/downloads failed against ParlInfo "
            f"— refusing to report success on a partial harvest. First failures: "
            f"{failures[:5]}"
        )
    if not found:
        raise RuntimeError(
            f"no transcripts found across {len(targets)} chamber-days in "
            f"{_years_to_rebuild(today)}. Parliament sits ~50-80 days per chamber "
            f"per year, so an empty year-to-date window means the probe is broken, "
            f"not that the chambers never sat."
        )

    print(f"downloaded {len(found):,} sitting-day transcripts")
    return {"input_dir": str(input_dir), "count": len(found)}


@task(log_prints=True)
def clean_hansard(work_dir: str, input_dir: str) -> dict:
    """Parse the downloaded transcripts into all-STRING Parquet partitions."""
    output_dir = Path(work_dir) / "output"
    speech_columns = _architecture_columns("speech")
    day_columns = _architecture_columns("sitting_day")

    speeches: dict[str, list[dict]] = defaultdict(list)
    days: dict[str, list[dict]] = defaultdict(list)
    max_date = None

    for path in sorted(Path(input_dir).rglob("*.xml")):
        house = path.relative_to(Path(input_dir)).parts[0]
        try:
            day, rows = parse_sitting_day(path.read_bytes(), house, path.name)
        except Exception as exc:
            print(f"  unparsed {path.name}: {type(exc).__name__}")
            continue
        year = day.get("year") or path.parent.name
        days[year].append(day)
        for row in rows:
            row["year"] = row.get("year") or year
            speeches[year].append(row)
        if day.get("date") and (max_date is None or day["date"] > max_date):
            max_date = day["date"]

    def dump(
        rows_by_year: dict[str, list[dict]], table: str, columns: list[str]
    ) -> str:
        payload = [column for column in columns if column != "year"]
        schema = pa.schema([pa.field(name, pa.string()) for name in payload])
        base = output_dir / table
        for year, rows in sorted(rows_by_year.items()):
            arrow = pa.Table.from_pydict(
                {
                    name: pa.array(
                        [row.get(name) for row in rows], type=pa.string()
                    )
                    for name in payload
                },
                schema=schema,
            )
            partition = base / f"year={year}"
            partition.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                arrow, partition / "data.parquet", compression="snappy"
            )
        return str(base)

    result = {
        "speech": dump(speeches, "speech", speech_columns),
        "sitting_day": dump(days, "sitting_day", day_columns),
        "max_date": max_date,
        "speech_rows": sum(len(v) for v in speeches.values()),
        "sitting_days": sum(len(v) for v in days.values()),
    }
    print(
        f"parsed {result['sitting_days']:,} sitting days, "
        f"{result['speech_rows']:,} utterances, latest {max_date}"
    )
    return result


@task(log_prints=True)
def cleanup(work_dir: str) -> None:
    shutil.rmtree(work_dir, ignore_errors=True)
