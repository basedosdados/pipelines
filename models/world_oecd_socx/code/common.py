"""Shared paths, constants and HTTP helpers for the world_oecd_socx onboarding.

Source is the OECD SDMX REST API (``sdmx.oecd.org/public/rest``), the same bulk
route used by ``world_oecd_education``. The dataset is the OECD Social Expenditure
Database (SOCX), Aggregated data: a single cube, ``DSD_SOCX_AGG@DF_SOCX_AGG``,
carrying public and private social expenditure by policy area, source and unit.
The ``DF_PUB_*`` and ``DF_NET_GDP`` dataflows in the same agency are views over
this cube, so they are documentation rather than separate tables. There is no
SOCX Detailed cube on SDMX (``DSD_SOCX_DET`` 404s).

Scratch data goes under ``~/Downloads/world_oecd_socx_data/`` (never in the repo
or in Dropbox), overridable via ``OECD_SOCX_DATA_DIR``. The architecture CSV under
``architecture/`` is the single source of truth for column names, order and types;
``gen_architecture.py`` writes it from the SDMX data structure definition.
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
        "OECD_SOCX_DATA_DIR",
        Path.home() / "Downloads" / "world_oecd_socx_data",
    )
)
INPUT = DATA_DIR / "input"
OUTPUT = DATA_DIR / "output"
STRUCTURE = INPUT / "structure"

DATASET_ID = "world_oecd_socx"
SDMX = "https://sdmx.oecd.org/public/rest"

# sdmx.oecd.org 403s the literal ``Python-urllib/3.x`` User-Agent. ``requests``
# sends its own and is fine, but pin one so the behaviour cannot drift.
HEADERS = {
    "User-Agent": "basedosdados-onboarding/1.0 (contato@basedosdados.org)"
}

# The API rate-limits by IP and answers 429 with a plain-text body. ``curl -o``
# and any client that ignores the status code will happily write that 246-byte
# body into a file named ``*.csv``; every fetch here goes through ``get`` so the
# status is always checked. The gap is adaptive: every 429 widens it for the rest
# of the run, so the throttle settles near whatever the server will serve.
REQUEST_GAP_S = 3.0
GAP_STEP_S = 1.5
MAX_GAP_S = 20.0
MAX_RETRIES = 8
BACKOFF_BASE_S = 20

_last_request = 0.0
_gap = REQUEST_GAP_S


class RateLimitError(RuntimeError):
    """The OECD API returned 429."""


# The API answers a query that matches no observations with 404 and this exact
# body, not with an empty 200. It is a legitimate "nothing here".
NO_RECORDS = "NoRecordsFound"


def get(url, *, params=None, stream=False, timeout=1800, allow_empty=False):
    """GET with UA, throttling, and retry-with-backoff on 429 and 5xx.

    Never returns a response whose status is not 200, so a caller cannot mistake
    an error body for data. With ``allow_empty``, a 404 whose body is exactly
    ``NoRecordsFound`` returns None instead — any other 404 still raises, so a
    genuinely broken URL cannot be silently recorded as an empty chunk.
    """
    global _last_request, _gap
    for attempt in range(MAX_RETRIES):
        gap = _gap - (time.monotonic() - _last_request)
        if gap > 0:
            time.sleep(gap)
        resp = requests.get(
            url, params=params, headers=HEADERS, stream=stream, timeout=timeout
        )
        _last_request = time.monotonic()
        if resp.status_code == 200:
            return resp
        if (
            allow_empty
            and resp.status_code == 404
            and resp.text.strip() == NO_RECORDS
        ):
            return None
        # Sustained heavy use escalates the IP to a Cloudflare managed challenge,
        # which answers 403 with a "Just a moment..." interstitial rather than the
        # plain-text 429. It is transient and clears once traffic stops, so treat
        # it like a rate-limit: widen the gap and back off, don't raise.
        cf_challenge = (
            resp.status_code == 403 and "Just a moment" in resp.text[:2000]
        )
        if resp.status_code == 429 or resp.status_code >= 500 or cf_challenge:
            if resp.status_code == 429 or cf_challenge:
                _gap = min(_gap + GAP_STEP_S, MAX_GAP_S)
            wait = BACKOFF_BASE_S * (2**attempt)
            print(
                f"  {resp.status_code} from OECD API, sleeping {wait}s, "
                f"gap now {_gap:.1f}s (attempt {attempt + 1}/{MAX_RETRIES})",
                flush=True,
            )
            time.sleep(wait)
            continue
        resp.raise_for_status()
    raise RateLimitError(
        f"still rate-limited after {MAX_RETRIES} attempts: {url}"
    )
