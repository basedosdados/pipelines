"""Pure refresh helpers for us_ffiec_bank_reporting.

No Prefect imports live here: `tasks.py` wraps these, and the one-shot
onboarding runners under `models/us_ffiec_bank_reporting/code/` call the same
`download` and `clean` modules, so the transform exists in exactly one place.

Everything here is about *which* periods to refresh. The parsing itself is
unchanged from the onboarding and lives in `clean.py`.
"""

from __future__ import annotations

from pipelines.datasets.us_ffiec_bank_reporting import clean, download
from pipelines.datasets.us_ffiec_bank_reporting.common import (
    CRA_FIRST_YEAR,
    quarters,
)


def latest_source_quarter() -> tuple[int, int]:
    """Newest Call Report quarter the CDR offers, as (year, quarter)."""
    return download.latest_call_period()


def quarter_to_coverage_date(period: tuple[int, int]) -> str:
    """(2026, 2) -> '2026-06', the month the quarter ends in.

    The poll compares this against the table's registered Coverage, and a
    year_quarter coverage is stored month-granular (DateFormat.YEAR_MONTH). The
    string granularity, the poll's date_format and the coverage granularity all
    have to agree -- a coarser string silently makes the pipeline annual.
    """
    year, quarter = period
    return f"{year:04d}-{3 * quarter:02d}"


def trailing_window(
    last: tuple[int, int], count: int
) -> tuple[tuple[int, int], tuple[int, int]]:
    """The `count` quarters ending at `last`, as a (first, last) pair."""
    span = quarters((last[0] - (count // 4) - 1, 1), last)[-count:]
    return span[0], span[-1]


def refresh_quarterly(last: tuple[int, int], trailing: int) -> None:
    """Download and re-clean the trailing quarters of Call Report and FR Y-9C.

    Re-cleaning rather than appending is deliberate: amended filings restate
    values already published, and each quarter is written to its own
    `year=YYYY/data_q<N>.parquet`, so a rerun replaces that object instead of
    adding a duplicate. That is what makes `dump_mode="append"` idempotent here.
    """
    first, _ = trailing_window(last, trailing)
    download.download_mdrm()
    download.download_call(first=first, last=last)
    download.download_bhc(first=first, last=last)
    items = clean.clean_mdrm()
    clean.clean_dictionary()
    clean.clean_call(items, limit=trailing, last=last)
    clean.clean_bhc(items, limit=trailing, last=last)


def refresh_cra(last_year: int, trailing_years: int) -> None:
    """Download and re-clean the trailing years of the CRA disclosure."""
    first_year = max(CRA_FIRST_YEAR, last_year - trailing_years + 1)
    download.download_cra(first_year=first_year, last_year=last_year)
    clean.clean_dictionary()
    clean.clean_cra(first_year=first_year, last_year=last_year)
