#!/usr/bin/env python3
"""Bootstrap: download every ABS price release's time-series workbooks.

The download and cleaning transforms live in
``pipelines.datasets.au_abs_prices_inflation`` so the one-shot bootstrap and
the recurring Prefect pipeline share one implementation. This CLI is the
initial-load entry point.

Usage:
    uv run python models/au_abs_prices_inflation/code/download.py [release ...]

With no arguments it downloads the Consumer Price Index plus all five other
releases. Files already present are left alone, so a re-run is cheap.
"""

import logging
import os
import sys
from pathlib import Path

from pipelines.datasets.au_abs_prices_inflation.constants import constants
from pipelines.datasets.au_abs_prices_inflation.cpi import (
    download_all as download_cpi,
)
from pipelines.datasets.au_abs_prices_inflation.timeseries import (
    download_release,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("au_abs_prices_inflation")

# Scratch data (raw downloads, cleaned parquet) never lives in the repo: the
# checkout sits inside Dropbox, so writing multi-GB output here would trigger a
# sync and risk committing data. Default to ~/Downloads and allow an override.
DATA_ROOT = Path(
    os.environ.get(
        "AU_ABS_PRICES_INFLATION_DATA",
        Path.home() / "Downloads" / "au_abs_prices_inflation_data",
    )
)


def _selected(argv, known, what):
    """Resolve the names requested on the command line against ``known``.

    An unrecognised name raises rather than silently selecting nothing: the
    loops below skip whatever they do not match, so a typo would otherwise
    exit 0 having done no work at all, which reads exactly like success.
    """
    want = set(argv)
    unknown = sorted(want - set(known))
    if unknown:
        raise SystemExit(
            f"unknown {what}: {', '.join(unknown)}\n"
            f"valid {what}: {', '.join(known)}"
        )
    return want


def main():
    """Download the releases named on the command line, or all of them."""
    releases = list(constants.RELEASES.value)
    want = _selected(sys.argv[1:], ["cpi", *releases], "release")
    inp = DATA_ROOT / "input"

    if not want or "cpi" in want:
        (inp / "cpi").mkdir(parents=True, exist_ok=True)
        # cpi.download_all returns its output directory, not the release slug,
        # so report what was actually fetched rather than mislabelling a path.
        download_cpi(str(inp / "cpi"))
        log.info("cpi: %d workbooks", len(list((inp / "cpi").glob("*.xlsx"))))

    for release in releases:
        if want and release not in want:
            continue
        slug, paths = download_release(release, str(inp))
        log.info("%s: release %s, %d workbooks", release, slug, len(paths))


if __name__ == "__main__":
    main()
