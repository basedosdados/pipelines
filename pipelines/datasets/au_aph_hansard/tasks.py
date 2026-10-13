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
    http_get,
    list_openaustralia_days,
    load_openaustralia_roster,
    openaustralia_day_url,
    parse_any,
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
    """Download every sitting day in the rebuild window from OpenAustralia.

    The source is OpenAustralia's mirror rather than ParlInfo: ParlInfo answers
    this worker with HTTP 403 on every request - 490 of 490 probes on
    2026-09-02 - while serving the identical code and headers from an
    Australian connection, so the block is on the egress IP.

    One directory listing per chamber replaces the 490 single-day searches the
    ParlInfo version issued per run.
    """
    today = today or date.today()
    input_dir = Path(work_dir) / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    wanted = set(_years_to_rebuild(today))

    targets: list[tuple[str, str, str]] = []
    for house in constants.OPENAUSTRALIA_DIRS.value:
        listing = list_openaustralia_days(house)
        for day, url in listing.items():
            if int(day[:4]) in wanted and day <= today.isoformat():
                targets.append((house, day, url))
        print(
            f"{house}: {len(listing):,} days published, {len(targets):,} in window so far"
        )

    outcomes: Counter[str] = Counter()
    failures: list[str] = []
    lock = Lock()

    def grab(item: tuple[str, str, str]) -> str | None:
        house, day, url = item
        try:
            payload = http_get(url)
        except Exception as exc:
            with lock:
                outcomes["download_failed"] += 1
                if len(failures) < 5:
                    failures.append(f"{house} {day}: {type(exc).__name__}")
            return None
        dest = input_dir / house / day[:4] / f"{day}.xml"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(payload)
        with lock:
            outcomes["downloaded"] += 1
        return str(dest)

    with ThreadPoolExecutor(max_workers=6) as pool:
        found = [path for path in pool.map(grab, targets) if path]

    print(f"download outcomes: {dict(outcomes)}")
    for line in failures:
        print(f"  ! {line}")

    # Never report success on a harvest that found nothing: a source that has
    # started refusing us looks exactly like a parliamentary recess unless the
    # difference is made to fail.
    if outcomes["download_failed"]:
        raise RuntimeError(
            f"{outcomes['download_failed']} of {len(targets)} downloads failed "
            f"against OpenAustralia. First failures: {failures[:5]}"
        )
    if not found:
        raise RuntimeError(
            f"no sitting days published for {sorted(wanted)} at OpenAustralia. "
            f"Parliament sits 50-80 days per chamber per year, so an empty "
            f"year-to-date window means the listing is broken, not that the "
            f"chambers never sat."
        )

    print(f"downloaded {len(found):,} sitting-day transcripts")
    return {"input_dir": str(input_dir), "count": len(found)}


@task(log_prints=True)
def clean_hansard(work_dir: str, input_dir: str) -> dict:
    """Parse the downloaded transcripts into all-STRING Parquet partitions."""
    output_dir = Path(work_dir) / "output"
    speech_columns = _architecture_columns("speech")
    day_columns = _architecture_columns("sitting_day")

    roster = load_openaustralia_roster()
    speeches: dict[str, list[dict]] = defaultdict(list)
    days: dict[str, list[dict]] = defaultdict(list)
    max_date = None

    for path in sorted(Path(input_dir).rglob("*.xml")):
        house = path.relative_to(Path(input_dir)).parts[0]
        try:
            # Record where the transcript came from, not its local filename:
            # source_url is how a reader tells which source built a sitting day.
            source_url = openaustralia_day_url(house, path.stem)
            day, rows = parse_any(path.read_bytes(), house, source_url, roster)
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
            filename = constants.PARTITION_FILE.value.format(year=year)
            pq.write_table(arrow, partition / filename, compression="snappy")
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
