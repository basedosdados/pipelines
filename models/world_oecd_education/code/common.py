"""Shared paths, constants and HTTP helpers for the world_oecd_education onboarding.

Source is the OECD SDMX REST API (``sdmx.oecd.org/public/rest``), not Education GPS.
GPS is a presentation layer: Cloudflare-gated, undocumented, and its own "Download
Indicator" links point back at the OECD Data Explorer, which is this same API.

Scratch data goes under ``~/Downloads/world_oecd_education_data/`` (never in the repo
or in Dropbox), overridable via ``OECD_EDU_DATA_DIR``. The architecture CSVs under
``architecture/`` are the single source of truth for column names, order and types;
``gen_architecture.py`` writes them from the SDMX data structure definitions.
"""

import os
import sys
import time
from pathlib import Path

import requests

CODE_DIR = Path(__file__).resolve().parent
ARCH_DIR = CODE_DIR / "architecture"
REPO_ROOT = CODE_DIR.parents[2]
# These scripts run from their own directory with bare sibling imports, so the
# repo root is not otherwise importable.
sys.path.insert(0, str(REPO_ROOT))

DATA_DIR = Path(
    os.environ.get(
        "OECD_EDU_DATA_DIR",
        Path.home() / "Downloads" / "world_oecd_education_data",
    )
)
INPUT = DATA_DIR / "input"
OUTPUT = DATA_DIR / "output"
STRUCTURE = INPUT / "structure"

DATASET_ID = "world_oecd_education"
SDMX = "https://sdmx.oecd.org/public/rest"

# sdmx.oecd.org 403s the literal ``Python-urllib/3.x`` User-Agent. ``requests``
# sends its own and is fine, but pin one so the behaviour cannot drift.
HEADERS = {
    "User-Agent": "basedosdados-onboarding/1.0 (contato@basedosdados.org)"
}

# The API rate-limits by IP and answers 429 with a plain-text body. ``curl -o``
# and any client that ignores the status code will happily write that 246-byte
# body into a file named ``*.csv``; every fetch here goes through ``get`` so the
# status is always checked. Tripped once during scoping at roughly 200 structure
# requests issued 0.15s apart, so keep the gap well above that.
REQUEST_GAP_S = 1.0
MAX_RETRIES = 6
BACKOFF_BASE_S = 30

_last_request = 0.0


class RateLimitError(RuntimeError):
    """The OECD API returned 429."""


def get(url, *, params=None, stream=False, timeout=1800):
    """GET with UA, throttling, and retry-with-backoff on 429 and 5xx.

    Never returns a response whose status is not 200, so a caller cannot mistake
    an error body for data.
    """
    global _last_request
    for attempt in range(MAX_RETRIES):
        gap = REQUEST_GAP_S - (time.monotonic() - _last_request)
        if gap > 0:
            time.sleep(gap)
        resp = requests.get(
            url, params=params, headers=HEADERS, stream=stream, timeout=timeout
        )
        _last_request = time.monotonic()
        if resp.status_code == 200:
            return resp
        if resp.status_code == 429 or resp.status_code >= 500:
            wait = BACKOFF_BASE_S * (2**attempt)
            print(
                f"  {resp.status_code} from OECD API, sleeping {wait}s "
                f"(attempt {attempt + 1}/{MAX_RETRIES}): {url}",
                flush=True,
            )
            time.sleep(wait)
            continue
        resp.raise_for_status()
    raise RateLimitError(
        f"still rate-limited after {MAX_RETRIES} attempts: {url}"
    )
