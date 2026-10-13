"""Pure download and cleaning helpers for br_cgu_orcamento_publico.

No Prefect imports here: ``tasks.py`` wraps these, and they can be run directly
for local parity checks against an already-downloaded ``input/``.

The transform itself is small — the source ships one ZIP per exercise, each
holding a single Latin-1, semicolon-separated CSV with 26 quoted columns, and
the only real work is normalising Brazilian number formatting ("23929428,72",
"86,16%") into something ``safe_cast`` accepts.

**The download is the delicate part.** ``portaldatransparencia.gov.br`` redirects
to ``dadosabertos-download.cgu.gov.br``, which sits behind CloudFront + AWS WAF
with a rate-based rule. A burst trips it and every later request comes back
``405`` with ``x-amzn-waf-action: captcha`` — an HTML CAPTCHA page, not the ZIP —
for a sustained period, from that IP, regardless of client. Measured 2026-09-11:
15 HEADs followed immediately by 13 GETs tripped it after three files, and it was
still refusing every request twelve seconds apart some minutes later. So this
module issues **one request per exercise**, spaced by ``REQUEST_DELAY``, and
reads ``Last-Modified`` off the download response rather than probing separately.
A CAPTCHA response raises :class:`SourceChallengedError` — it is never solved or
worked around.
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

import requests

from pipelines.datasets.br_cgu_orcamento_publico.constants import constants

log = logging.getLogger(__name__)

BASE_URL = constants.BASE_URL.value
USER_AGENT = constants.USER_AGENT.value
FIRST_YEAR = constants.FIRST_YEAR.value
ENCODING = constants.SOURCE_ENCODING.value
DELIMITER = constants.SOURCE_DELIMITER.value
STAGING_COLUMNS = constants.STAGING_COLUMNS.value
HEADER_TOKENS = constants.HEADER_TOKENS.value
NUMERIC_COLUMNS = set(constants.NUMERIC_COLUMNS.value)

# Seconds between requests to the CGU download host. Generous on purpose: the
# whole source is ~10 MB across 13 files, so pacing costs a minute and buys us
# not being CAPTCHA-blocked for the rest of the run.
REQUEST_DELAY = 5.0
_TIMEOUT = 300


class SourceChallengedError(RuntimeError):
    """AWS WAF answered with a CAPTCHA challenge instead of the file.

    Raised so the run fails loudly and visibly rather than silently writing a
    2 KB HTML page as if it were data. The remedy is to wait out the block and
    re-run, not to defeat the challenge.
    """


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def _fold(text: str) -> str:
    """Accent-fold and snake-case a source header for comparison."""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", text.lower())).strip(
        "_"
    )


def year_url(year: int) -> str:
    return f"{BASE_URL}/{year}"


def _check_not_challenged(response: requests.Response, year: int) -> None:
    if response.headers.get("x-amzn-waf-action") == "captcha":
        raise SourceChallengedError(
            f"{year}: the CGU download host answered with an AWS WAF CAPTCHA "
            f"(HTTP {response.status_code}). The IP is rate-limited; wait for "
            "the block to lapse and re-run. Do not try to solve the challenge."
        )


def _parse_last_modified(response: requests.Response) -> datetime:
    stamp = response.headers.get("Last-Modified")
    if not stamp:
        # Published but undated; treat as modified now so the run proceeds.
        return datetime.now(UTC)
    return datetime.strptime(stamp, "%a, %d %b %Y %H:%M:%S %Z").replace(
        tzinfo=UTC
    )


def probe_latest(session: requests.Session | None = None) -> str:
    """Return the newest exercise ZIP's ``Last-Modified`` as ``YYYY-MM-DD``.

    Costs a single HEAD. The portal regenerates every exercise file in the same
    pass — 2024 and 2026 both carried ``Wed, 09 Sep 2026 08:03:12 GMT`` — so one
    file's timestamp stands for the whole release, and the flow can decide
    whether to run before spending the download budget.

    Falls back to the previous exercise when the current one is not published
    yet, which is the state between January 1st and the LOA's publication.
    """
    session = session or _session()
    this_year = datetime.now(UTC).year
    for year in (this_year, this_year - 1):
        r = session.head(year_url(year), allow_redirects=True, timeout=60)
        _check_not_challenged(r, year)
        if r.status_code == 200:
            return _parse_last_modified(r).date().isoformat()
    raise RuntimeError(
        f"Neither exercise {this_year} nor {this_year - 1} is published under "
        f"{BASE_URL} — the source layout changed"
    )


def download_all(
    input_dir: Path, session: requests.Session | None = None
) -> list[int]:
    """Download every published exercise ZIP into ``input_dir``.

    Walks forward from :data:`FIRST_YEAR` and stops at the first exercise the
    portal does not publish, which is how the year range is discovered rather
    than hardcoded. A 403 is only accepted as the end of the range once we are
    at or past the current year — a gap in the middle of the history is a source
    problem, not a stopping condition, and raises.

    Returns the sorted list of exercises actually downloaded.
    """
    session = session or _session()
    input_dir.mkdir(parents=True, exist_ok=True)
    this_year = datetime.now(UTC).year
    years: list[int] = []

    for i, year in enumerate(range(FIRST_YEAR, this_year + 3)):
        if i:
            time.sleep(REQUEST_DELAY)
        r = session.get(year_url(year), timeout=_TIMEOUT)
        _check_not_challenged(r, year)
        if r.status_code != 200:
            if year >= this_year:
                log.info(
                    "%d not published (HTTP %d) — end of range",
                    year,
                    r.status_code,
                )
                break
            raise RuntimeError(
                f"{year}: HTTP {r.status_code} for an exercise that should exist "
                f"(the history runs from {FIRST_YEAR} without gaps)"
            )
        _extract(r.content, year, input_dir)
        years.append(year)

    if not years:
        raise RuntimeError(f"No exercise downloaded from {BASE_URL}")
    log.info("downloaded %d exercises: %s", len(years), years)
    return years


def _extract(payload: bytes, year: int, input_dir: Path) -> Path:
    """Extract the single CSV member of one exercise archive."""
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if len(members) != 1:
            raise RuntimeError(
                f"{year}: expected exactly one CSV in the archive, got {members}"
            )
        target = input_dir / f"{year}.csv"
        with zf.open(members[0]) as src, open(target, "wb") as dst:
            dst.write(src.read())
    log.info("%d: %s (%d bytes)", year, target.name, target.stat().st_size)
    return target


def _check_header(year: int, header: list[str]) -> None:
    """Fail loudly when the source layout drifts from what the model expects."""
    if len(header) != len(HEADER_TOKENS):
        raise RuntimeError(
            f"{year}: expected {len(HEADER_TOKENS)} columns, got {len(header)}: {header}"
        )
    for pos, (raw, token) in enumerate(
        zip(header, HEADER_TOKENS, strict=True)
    ):
        if not _fold(raw).startswith(token):
            raise RuntimeError(
                f"{year}: column {pos} is {raw!r} (folded {_fold(raw)!r}), "
                f"expected something starting with {token!r}"
            )


def parse_number(value: str) -> str:
    """Normalise a Brazilian-formatted number into a ``safe_cast``-able string.

    ``"23929428,72"`` -> ``"23929428.72"``, ``"86,16%"`` -> ``"86.16"``,
    ``"-23657%"`` -> ``"-23657"``. A blank stays blank, which the CSV writes as
    an empty field and BigQuery reads as NULL — never the literal ``"nan"``.
    """
    value = value.strip().rstrip("%").strip()
    if not value:
        return ""
    # "." can only be a thousands separator here; the decimal mark is ",".
    return value.replace(".", "").replace(",", ".")


def clean_year(csv_path: Path, year: int, output_dir: Path) -> int:
    """Clean one exercise into ``<output_dir>/ano_exercicio=<year>/data.csv``.

    Returns the number of data rows written. The exercise column is dropped from
    the file because it is carried by the hive partition key.
    """
    part_dir = output_dir / f"ano_exercicio={year}"
    part_dir.mkdir(parents=True, exist_ok=True)
    target = part_dir / "data.csv"

    written = 0
    with (
        open(csv_path, encoding=ENCODING, newline="") as fh,
        open(target, "w", encoding="utf-8", newline="") as out,
    ):
        reader = csv.reader(fh, delimiter=DELIMITER)
        writer = csv.writer(out, lineterminator="\n")
        _check_header(year, next(reader))
        writer.writerow(STAGING_COLUMNS)
        for row in reader:
            if len(row) != len(HEADER_TOKENS):
                raise RuntimeError(
                    f"{year}: row {written + 2} has {len(row)} fields, "
                    f"expected {len(HEADER_TOKENS)}"
                )
            exercise, rest = row[0].strip(), row[1:]
            if exercise != str(year):
                raise RuntimeError(
                    f"{year}: row {written + 2} carries exercise {exercise!r}"
                )
            writer.writerow(
                [
                    parse_number(v) if name in NUMERIC_COLUMNS else v.strip()
                    for name, v in zip(STAGING_COLUMNS, rest, strict=True)
                ]
            )
            written += 1
    if written == 0:
        raise RuntimeError(
            f"{year}: cleaned to zero rows — an empty partition would poison "
            "the staging schema, so this is an error, not an empty result"
        )
    log.info("%d: %d rows -> %s", year, written, target)
    return written


def clean_all(input_dir: Path, output_dir: Path, years: list[int]) -> dict:
    """Clean every downloaded exercise into one hive-partitioned directory.

    Returns ``{"path": <table dir>, "rows": {year: n}, "total": n}``.
    """
    table_dir = output_dir / constants.TABLE_ID.value
    rows = {
        year: clean_year(input_dir / f"{year}.csv", year, table_dir)
        for year in sorted(years)
    }
    total = sum(rows.values())
    log.info("cleaned %d exercises, %d rows total", len(rows), total)
    return {"path": table_dir, "rows": rows, "total": total}
