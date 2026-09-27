#!/usr/bin/env python3
"""Download the OECD Global Revenue Statistics comparative cube, chunked by area.

Codes-only CSV (``format=csvfile``), one file per REF_AREA under
``<data>/input/``. Resume-safe: an area whose file already exists and is non-empty
is skipped. The OECD API rate-limits on request volume and frequency, so this uses
the shared adaptive throttle in ``utils.get`` (every 429 widens the gap for the
rest of the run). A full run takes 1-3 hours; it is meant to run in the background.

Usage:
    uv run python models/world_oecd_revenue_statistics/code/download.py
"""

import io
import logging
import os
from pathlib import Path

import pandas as pd

from pipelines.datasets.world_oecd_revenue_statistics import utils
from pipelines.datasets.world_oecd_revenue_statistics.constants import (
    constants,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("world_oecd_revenue_statistics")

DATA_DIR = Path(
    os.environ.get(
        "OECD_REV_DATA_DIR",
        Path.home() / "Downloads" / "world_oecd_revenue_statistics_data",
    )
)
INPUT = DATA_DIR / "input"
FLOW_REF = constants.FLOW_REF.value
VERSION = constants.DEFAULT_VERSION.value
BASE = f"{constants.SDMX_BASE.value}/data/{FLOW_REF},{VERSION}"

# The IP is penalty-prone under load; start conservative.
utils._state["gap"] = 45.0


def area_universe() -> list:
    cache = INPUT / "_areas.txt"
    if cache.exists():
        return [a for a in cache.read_text().split() if a]
    body = utils.get(
        f"{BASE}/......?format=csvfile&startPeriod=2021&endPeriod=2021"
    )
    areas = sorted(
        {
            r["REF_AREA"]
            for r in pd.read_csv(io.StringIO(body), dtype=str).to_dict(
                "records"
            )
        }
    )
    INPUT.mkdir(parents=True, exist_ok=True)
    cache.write_text("\n".join(areas))
    return areas


def main():
    INPUT.mkdir(parents=True, exist_ok=True)
    areas = area_universe()
    log.info("%d areas", len(areas))
    done = 0
    for i, a in enumerate(areas, 1):
        dst = INPUT / f"{a}.csv"
        if dst.exists() and dst.stat().st_size > 0:
            log.info("[%d/%d] %s cached", i, len(areas), a)
            done += 1
            continue
        body = utils.get(f"{BASE}/{a}......?format=csvfile")
        if body and not body.startswith(("Could not", "No Results")):
            dst.write_text(body)
            log.info(
                "[%d/%d] %s: %d rows", i, len(areas), a, body.count("\n") - 1
            )
            done += 1
        else:
            log.warning("[%d/%d] %s: empty/failed", i, len(areas), a)
    log.info("done: %d/%d areas -> %s", done, len(areas), INPUT)


if __name__ == "__main__":
    main()
