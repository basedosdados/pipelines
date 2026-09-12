"""Pure download and cleaning helpers for br_cgu_despesas_publicas.

No Prefect imports here, so the one-shot onboarding under
``models/br_cgu_despesas_publicas/code/`` and the recurring flow share exactly
one copy of the transform.

Schema comes from the architecture CSV
(``models/br_cgu_despesas_publicas/code/architecture/execucao.csv``), which is
the single source of truth for column order, types and the source-header
mapping. Nothing here hardcodes a column list.

**Pacing is mandatory.** The CGU download host is CloudFront + AWS WAF with a
rate rule; a burst earns a 405 ``x-amzn-waf-action: captcha`` HTML page instead
of the ZIP, IP-wide, for 6-9 minutes. One request per month, spaced by
``REQUEST_DELAY``, and a challenge raises rather than being written to disk.
"""

from __future__ import annotations

import csv
import io
import logging
import re
import time
import unicodedata
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.br_cgu_despesas_publicas.constants import constants

log = logging.getLogger(__name__)

BASE_URL = constants.BASE_URL.value
USER_AGENT = constants.USER_AGENT.value
ENCODING = constants.SOURCE_ENCODING.value
DELIMITER = constants.SOURCE_DELIMITER.value
PERIOD_COLUMN = constants.PERIOD_COLUMN.value
REQUEST_DELAY = constants.REQUEST_DELAY.value
FIRST_YEAR = constants.FIRST_YEAR.value
FIRST_MONTH = constants.FIRST_MONTH.value
BATCH_SIZE = constants.BATCH_SIZE.value
BATCH_PAUSE = constants.BATCH_PAUSE.value
BLOCK_PAUSE = constants.BLOCK_PAUSE.value
MAX_BLOCK_RETRIES = constants.MAX_BLOCK_RETRIES.value

_TIMEOUT = 600
# Hive partition keys; carried by the directory path, not by the parquet file.
PARTITION_COLUMNS = ("ano", "mes")


class SourceChallengedError(RuntimeError):
    """AWS WAF answered with a CAPTCHA challenge instead of the file.

    Raised so a run fails loudly rather than persisting a 2 KB HTML page as if
    it were data. The remedy is to wait out the block and re-run; the challenge
    is never solved or circumvented.
    """


def read_architecture() -> list[dict]:
    """Load the architecture CSV — column order, types and source mapping."""
    path = constants.ARCHITECTURE_DIR.value / "execucao.csv"
    with open(path, encoding="utf-8", newline="") as fh:
        arch = list(csv.DictReader(fh))
    if not arch:
        raise RuntimeError(f"{path} is empty")
    return arch


def _fold(text: str) -> str:
    """Accent-fold and snake-case a source header for comparison."""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", text.lower())).strip(
        "_"
    )


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def month_url(year: int, month: int) -> str:
    return f"{BASE_URL}/{year}{month:02d}"


def _check_not_challenged(response: requests.Response, label: str) -> None:
    if response.headers.get("x-amzn-waf-action") == "captcha":
        raise SourceChallengedError(
            f"{label}: the CGU download host answered with an AWS WAF CAPTCHA "
            f"(HTTP {response.status_code}). The IP is rate-limited; wait for "
            "the block to lapse and re-run. Do not try to solve the challenge."
        )


def month_range(
    first: tuple[int, int] | None = None, last: tuple[int, int] | None = None
) -> list[tuple[int, int]]:
    """Every (year, month) from ``first`` through ``last``, inclusive."""
    y0, m0 = first or (FIRST_YEAR, FIRST_MONTH)
    if last is None:
        now = datetime.now(UTC)
        last = (now.year, now.month)
    y1, m1 = last
    out = []
    y, m = y0, m0
    while (y, m) <= (y1, m1):
        out.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def _parse_last_modified(response: requests.Response) -> datetime:
    stamp = response.headers.get("Last-Modified")
    if not stamp:
        # Published but undated; treat as modified now so the run proceeds.
        return datetime.now(UTC)
    return datetime.strptime(stamp, "%a, %d %b %Y %H:%M:%S %Z").replace(
        tzinfo=UTC
    )


def probe_latest(session: requests.Session | None = None) -> str:
    """Return the current month's ``Last-Modified`` as ``YYYY-MM-DD``.

    Costs a single HEAD. Each month here carries its own timestamp — 2014-01 was
    last touched 2024-06-24 while 2026-01 was touched 2026-09-04 — so unlike
    ``orcamento-despesa`` one file does NOT speak for the whole release. The
    current month is the one that moves on every publication, which makes it the
    right freshness signal for a scheduled run; restatements of older months are
    picked up by the trailing refresh window instead.

    Falls back to the previous month when the current one is not published yet,
    which is the state in the first days of a month.
    """
    session = session or _session()
    now = datetime.now(UTC)
    candidates = [(now.year, now.month)]
    prev = now.year * 12 + (now.month - 1) - 1
    candidates.append((prev // 12, prev % 12 + 1))
    for year, month in candidates:
        r = session.head(
            month_url(year, month), allow_redirects=True, timeout=60
        )
        _check_not_challenged(r, f"{year}-{month:02d}")
        if r.status_code == 200:
            return _parse_last_modified(r).date().isoformat()
    raise RuntimeError(
        f"Neither {candidates[0]} nor {candidates[1]} is published under "
        f"{BASE_URL} — the source layout changed"
    )


def download_month(
    year: int,
    month: int,
    input_dir: Path,
    session: requests.Session | None = None,
    skip_existing: bool = True,
) -> Path | None:
    """Download one month's ZIP and extract its CSV into ``input_dir``.

    Returns the extracted CSV path, or None when the portal does not publish
    that month (HTTP 403), which is how the upper bound is discovered.

    ``skip_existing`` makes a re-run resumable after a WAF block without
    re-fetching what is already on disk.
    """
    target = input_dir / f"{year}{month:02d}.csv"
    if skip_existing and target.exists() and target.stat().st_size > 0:
        log.info("%d-%02d: already on disk, skipping", year, month)
        return target

    session = session or _session()
    input_dir.mkdir(parents=True, exist_ok=True)
    label = f"{year}-{month:02d}"
    r = session.get(month_url(year, month), timeout=_TIMEOUT)
    _check_not_challenged(r, label)
    if r.status_code == 403:
        log.info("%s: not published (HTTP 403)", label)
        return None
    r.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if len(members) != 1:
            raise RuntimeError(
                f"{label}: expected exactly one CSV in the archive, got {members}"
            )
        with zf.open(members[0]) as src, open(target, "wb") as dst:
            dst.write(src.read())
    log.info("%s: %s (%d bytes)", label, target.name, target.stat().st_size)
    return target


def download_all(
    input_dir: Path,
    months: list[tuple[int, int]] | None = None,
    session: requests.Session | None = None,
) -> list[tuple[int, int]]:
    """Download every published month, pacing to stay under the WAF rate rule.

    Requests go out in batches of ``BATCH_SIZE`` spaced ``REQUEST_DELAY`` apart,
    with ``BATCH_PAUSE`` between batches — a sustained rate well under the
    measured budget of ~22 requests per four minutes. A challenge that still
    gets through is retried after ``BLOCK_PAUSE`` rather than failing the run,
    because the block lapses on its own in 6-9 minutes and everything already
    fetched is skipped on the retry.

    Stops at the first month the portal does not publish. A 403 before the
    current month is a source problem, not a stopping condition, and raises.
    """
    session = session or _session()
    now = datetime.now(UTC)
    wanted = list(months or month_range())
    got: list[tuple[int, int]] = []
    blocked = 0
    requests_made = 0
    i = 0

    while i < len(wanted):
        year, month = wanted[i]
        target = input_dir / f"{year}{month:02d}.csv"
        already = target.exists() and target.stat().st_size > 0

        if not already:
            if requests_made and requests_made % BATCH_SIZE == 0:
                log.info(
                    "batch of %d done — pausing %.0fs to stay under the WAF rate rule",
                    BATCH_SIZE,
                    BATCH_PAUSE,
                )
                time.sleep(BATCH_PAUSE)
            elif requests_made:
                time.sleep(REQUEST_DELAY)

        try:
            path = download_month(year, month, input_dir, session=session)
        except SourceChallengedError as exc:
            blocked += 1
            if blocked > MAX_BLOCK_RETRIES:
                raise
            log.warning(
                "%s — waiting %.0fs before resuming (block %d/%d)",
                exc,
                BLOCK_PAUSE,
                blocked,
                MAX_BLOCK_RETRIES,
            )
            time.sleep(BLOCK_PAUSE)
            session = _session()
            requests_made = 0
            continue

        if not already:
            requests_made += 1
        if path is None:
            if (year, month) >= (now.year, now.month):
                break
            raise RuntimeError(
                f"{year}-{month:02d}: not published, but it precedes the "
                "current month — the source has a gap"
            )
        got.append((year, month))
        i += 1

    if not got:
        raise RuntimeError(f"No month downloaded from {BASE_URL}")
    log.info(
        "downloaded %d months: %s .. %s (%d WAF blocks waited out)",
        len(got),
        got[0],
        got[-1],
        blocked,
    )
    return got


def parse_number(value: str) -> str:
    """Normalise a Brazilian-formatted number into a ``safe_cast``-able string.

    ``"23929428,72"`` -> ``"23929428.72"``. A blank stays blank, which parquet
    stores as NULL — never the literal ``"nan"``.
    """
    value = value.strip()
    if not value:
        return ""
    return value.replace(".", "").replace(",", ".")


def _check_header(label: str, header: list[str], arch: list[dict]) -> None:
    """Fail loudly when the source layout drifts from the architecture."""
    expected = [PERIOD_COLUMN] + [
        a["original_name"] for a in arch if a["name"] not in PARTITION_COLUMNS
    ]
    if len(header) != len(expected):
        raise RuntimeError(
            f"{label}: expected {len(expected)} columns, got {len(header)}"
        )
    for pos, (raw, want) in enumerate(zip(header, expected, strict=True)):
        if _fold(raw) != _fold(want):
            raise RuntimeError(
                f"{label}: column {pos} is {raw!r}, expected {want!r}"
            )


def clean_month(
    csv_path: Path, year: int, month: int, output_dir: Path, arch: list[dict]
) -> int:
    """Clean one month into ``<output_dir>/ano=<y>/mes=<m>/data.parquet``.

    The parquet is written **all-STRING** by house convention: staging is
    all-STRING and the dbt model ``safe_cast``s every column, and
    ``gcs.dump_header`` stringifies the header file BigQuery infers the staging
    schema from — so typed parquet would be rejected on read. The cast goes
    through arrow, never ``astype(str)``, so NULLs stay NULL.

    ``ano`` and ``mes`` are omitted from the file: they are the hive partition
    keys, carried by the directory path.
    """
    label = f"{year}-{month:02d}"
    data_cols = [a for a in arch if a["name"] not in PARTITION_COLUMNS]
    names = [a["name"] for a in data_cols]
    numeric = {a["name"] for a in data_cols if a["bigquery_type"] == "FLOAT64"}

    columns: dict[str, list] = {n: [] for n in names}
    rows = 0
    with open(csv_path, encoding=ENCODING, newline="") as fh:
        reader = csv.reader(fh, delimiter=DELIMITER)
        _check_header(label, next(reader), arch)
        for raw in reader:
            if len(raw) != len(names) + 1:
                raise RuntimeError(
                    f"{label}: row {rows + 2} has {len(raw)} fields, "
                    f"expected {len(names) + 1}"
                )
            period = raw[0].strip()
            if period != f"{year}/{month:02d}":
                raise RuntimeError(
                    f"{label}: row {rows + 2} carries period {period!r}"
                )
            for name, value in zip(names, raw[1:], strict=True):
                v = value.strip()
                columns[name].append(
                    (parse_number(v) if name in numeric else v) or None
                )
            rows += 1

    if rows == 0:
        raise RuntimeError(
            f"{label}: cleaned to zero rows — an empty partition would poison "
            "the staging schema, so this is an error, not an empty result"
        )

    part_dir = output_dir / f"ano={year}" / f"mes={month}"
    part_dir.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([pa.field(n, pa.string()) for n in names])
    table = pa.table(
        {n: pa.array(columns[n], type=pa.string()) for n in names},
        schema=schema,
    )
    pq.write_table(table, part_dir / "data.parquet", compression="snappy")
    log.info("%s: %d rows -> %s", label, rows, part_dir)
    return rows


def clean_all(
    input_dir: Path, output_dir: Path, months: list[tuple[int, int]]
) -> dict:
    """Clean every downloaded month into one hive-partitioned directory.

    Returns ``{"path": <table dir>, "rows": {"YYYY-MM": n}, "total": n,
    "max_period": "YYYY-MM"}``.
    """
    arch = read_architecture()
    table_dir = output_dir / constants.TABLE_ID.value
    rows = {}
    for year, month in sorted(months):
        rows[f"{year}-{month:02d}"] = clean_month(
            input_dir / f"{year}{month:02d}.csv", year, month, table_dir, arch
        )
    total = sum(rows.values())
    log.info("cleaned %d months, %d rows total", len(rows), total)
    return {
        "path": table_dir,
        "rows": rows,
        "total": total,
        "max_period": max(rows) if rows else None,
    }
