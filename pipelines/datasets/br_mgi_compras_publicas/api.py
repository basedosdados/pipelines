"""HTTP client and window generators for the Compras.gov.br API.

Pure functions -- no Prefect imports -- so the one-shot onboarding driver under
``models/br_mgi_compras_publicas/code/`` and the recurring flow share one
implementation.
"""

from __future__ import annotations

import datetime as dt
import logging
import re
import threading
import time
from collections.abc import Iterator
from typing import Any

import requests

from pipelines.datasets.br_mgi_compras_publicas.constants import (
    WindowKind,
    constants,
)

logger = logging.getLogger(__name__)

BASE_URL = constants.BASE_URL.value
PAGE_SIZE = constants.PAGE_SIZE.value
MAX_RETRIES = constants.MAX_RETRIES.value
BACKOFF_BASE = constants.BACKOFF_BASE.value
REQUEST_TIMEOUT = constants.REQUEST_TIMEOUT.value
MAX_WINDOW_DAYS = constants.MAX_WINDOW_DAYS.value
MAX_THROTTLE_RETRIES = constants.MAX_THROTTLE_RETRIES.value


class ComprasApiError(RuntimeError):
    """A request failed in a way retrying will not fix."""


# The API rate-limits with a 429 whose body names the cooldown, e.g.
# {"statusCode": 429, "message": "Rate limit is exceeded. Try again in 26 seconds."}
# There is no Retry-After header, so the delay has to be read out of the message.
_RETRY_SECONDS = re.compile(r"in (\d+) seconds?", re.IGNORECASE)

#: Longest a single 429 may park a worker. The API asks for up to 26 seconds,
#: but the substantive response to being throttled is cutting the request rate,
#: which `penalise()` does immediately and for every subsequent request. Serving
#: the full cooldown on top of that idles eight workers for the same overshoot
#: and was measured to cost roughly a 40x slowdown on /modulo-contratos/.
THROTTLE_SLEEP_CAP = 10.0


def retry_after_seconds(
    response: requests.Response, default: float = 30.0
) -> float:
    """Seconds to wait before retrying a rate-limited request."""
    header = response.headers.get("Retry-After")
    if header:
        try:
            return float(header)
        except ValueError:
            pass
    match = _RETRY_SECONDS.search(response.text or "")
    if match:
        return float(match.group(1)) + 1.0
    return default


class AdaptiveRateLimiter:
    """Self-tuning request pacer, one bucket per API module.

    The API rate-limits with HTTP 429 and the ceiling differs sharply by module:
    `/modulo-legado/` sustains several requests a second while
    `/modulo-contratos/` returns 429 almost continuously at six workers. Rather
    than hard-coding a guess per module, each bucket starts optimistic and
    converges: additive increase on sustained success, multiplicative decrease
    on every 429 (AIMD).

    Pacing is global across worker threads, so raising `MAX_WORKERS` changes how
    much work is in flight, not how fast requests are issued.

    The constants are a compromise found by measurement, in both directions.
    Too timid a climb (+0.05 per 20 successes against a halving decrease) needs
    1,400 clean requests to recover from a single 429, and pinned `modulo-legado`
    at 1.4 req/s when a fixed 3 req/s serves 96 consecutive requests with no 429
    at all. Too bold a pair (+0.2 per 10 against a 0.8 decrease) keeps the rate
    grazing the limit, and since each 429 costs a cooldown, `modulo-contratos`
    collapsed from 34 completed jobs a minute to under one. What makes probing
    upward affordable is capping that cooldown -- see `THROTTLE_SLEEP_CAP`.
    """

    def __init__(
        self,
        rate: float = 4.0,
        min_rate: float = 0.2,
        max_rate: float = 8.0,
        increase: float = 0.15,
        decrease: float = 0.7,
        successes_before_increase: int = 20,
    ) -> None:
        self._lock = threading.Lock()
        self._rate = rate
        self._min_rate = min_rate
        self._max_rate = max_rate
        self._increase = increase
        self._decrease = decrease
        self._successes_before_increase = successes_before_increase
        self._successes = 0
        self._next_slot = 0.0
        self._throttled = 0
        self._served = 0

    @property
    def rate(self) -> float:
        return self._rate

    @property
    def stats(self) -> tuple[int, int]:
        """(requests served, times rate-limited) since the process started."""
        return self._served, self._throttled

    def acquire(self) -> None:
        """Block until this thread may issue its request."""
        while True:
            with self._lock:
                now = time.monotonic()
                interval = 1.0 / self._rate
                start = max(now, self._next_slot)
                self._next_slot = start + interval
                wait = start - now
            if wait <= 0:
                return
            time.sleep(wait)
            return

    def penalise(self) -> None:
        with self._lock:
            self._throttled += 1
            self._successes = 0
            self._rate = max(self._min_rate, self._rate * self._decrease)
            # Push the next slot out so in-flight threads do not immediately
            # re-trigger the limit.
            self._next_slot = (
                max(self._next_slot, time.monotonic()) + 1.0 / self._rate
            )

    def reward(self) -> None:
        with self._lock:
            self._served += 1
            self._successes += 1
            if self._successes >= self._successes_before_increase:
                self._successes = 0
                self._rate = min(self._max_rate, self._rate + self._increase)


_LIMITERS: dict[str, AdaptiveRateLimiter] = {}
_LIMITERS_LOCK = threading.Lock()


def limiter_for(path: str) -> AdaptiveRateLimiter:
    """Return the shared limiter for the API module owning `path`."""
    module = path.strip("/").split("/", 1)[0] or "root"
    with _LIMITERS_LOCK:
        if module not in _LIMITERS:
            _LIMITERS[module] = AdaptiveRateLimiter()
        return _LIMITERS[module]


def limiter_rates() -> dict[str, str]:
    """Converged rate and throttle share per module, for logging.

    The throttle share is the number worth watching: a module sitting at a low
    rate with almost no 429s is being paced too conservatively, which is exactly
    the failure the AIMD constants were retuned to avoid.
    """
    with _LIMITERS_LOCK:
        out = {}
        for name, lim in _LIMITERS.items():
            served, throttled = lim.stats
            share = 100 * throttled / max(served + throttled, 1)
            out[name] = f"{lim.rate:.2f}/s {share:.0f}%429"
        return out


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "accept": "application/json",
            "user-agent": "basedosdados/br_mgi_compras_publicas (+https://basedosdados.org)",
        }
    )
    return session


def _sleep(attempt: int) -> None:
    time.sleep(min(BACKOFF_BASE**attempt, 60.0))


def fetch_page(
    session: requests.Session,
    path: str,
    params: dict[str, Any],
    *,
    timeout: int = REQUEST_TIMEOUT,
) -> dict[str, Any]:
    """Fetch one page, retrying transient failures.

    Returns the decoded envelope. An empty result is normalised to a zero-row
    envelope rather than raising: the API signals "nothing here" as HTTP 200
    with ``totalRegistros: 0``, and occasionally as a 204 or an empty body.
    """
    url = f"{BASE_URL}{path}"
    limiter = limiter_for(path)
    last_error: Exception | None = None
    attempt = 0
    throttled = 0
    while attempt < MAX_RETRIES and throttled < MAX_THROTTLE_RETRIES:
        limiter.acquire()
        try:
            response = session.get(url, params=params, timeout=timeout)
        except (
            requests.RequestException
        ) as exc:  # connection reset, read timeout
            last_error = exc
            attempt += 1
            _sleep(attempt)
            continue

        if response.status_code == 429:
            # Being paced is expected, not a failure: it must not consume the
            # budget reserved for genuine transient errors, or a slow endpoint
            # aborts the harvest.
            limiter.penalise()
            throttled += 1
            time.sleep(
                min(
                    retry_after_seconds(response, default=5.0),
                    THROTTLE_SLEEP_CAP,
                )
            )
            last_error = ComprasApiError(f"429 from {path}")
            continue

        if response.status_code == 400:
            # Client errors here are contract violations (page size out of
            # range, window over 365 days) and are served as plain text.
            raise ComprasApiError(
                f"400 from {path}: {response.text[:300]} params={params}"
            )
        if response.status_code in (204,) or not response.content:
            limiter.reward()
            return {
                "resultado": [],
                "totalRegistros": 0,
                "paginasRestantes": 0,
            }
        if response.status_code >= 500:
            last_error = ComprasApiError(f"{response.status_code} from {path}")
            attempt += 1
            _sleep(attempt)
            continue
        if response.status_code != 200:
            raise ComprasApiError(
                f"{response.status_code} from {path}: {response.text[:300]}"
            )

        try:
            payload = response.json()
        except ValueError:
            last_error = ComprasApiError(
                f"non-JSON body from {path}: {response.text[:200]}"
            )
            attempt += 1
            _sleep(attempt)
            continue

        if not isinstance(payload, dict):
            raise ComprasApiError(
                f"unexpected payload type {type(payload)} from {path}"
            )
        payload.setdefault("resultado", [])
        payload.setdefault("totalRegistros", len(payload["resultado"]))
        limiter.reward()
        return payload

    raise ComprasApiError(
        f"gave up on {path} after {attempt} errors and {throttled} rate limits: {last_error}"
    )


def iter_records(
    session: requests.Session,
    path: str,
    params: dict[str, Any],
    *,
    page_size: int = PAGE_SIZE,
) -> Iterator[dict[str, Any]]:
    """Yield every record for one query, following pagination to the end.

    ``paginasRestantes`` is authoritative; a short page is not a reliable
    end-of-stream signal on this API.
    """
    page = 1
    while True:
        envelope = fetch_page(
            session,
            path,
            {**params, "pagina": page, "tamanhoPagina": page_size},
        )
        rows = envelope.get("resultado") or []
        yield from rows
        if not rows or envelope.get("paginasRestantes", 0) <= 0:
            return
        page += 1


# --------------------------------------------------------------------------
# Window generators
# --------------------------------------------------------------------------


def _dates(
    start: dt.date, end: dt.date, step_days: int
) -> Iterator[tuple[dt.date, dt.date]]:
    cursor = start
    while cursor <= end:
        stop = min(cursor + dt.timedelta(days=step_days - 1), end)
        yield cursor, stop
        cursor = stop + dt.timedelta(days=1)


def windows(
    kind: WindowKind,
    start: dt.date,
    end: dt.date,
    step_days: int,
) -> list[tuple[str, str, str]]:
    """Build ``(label, param_inicial, param_final)`` triples covering [start, end].

    The two regimes disagree about the upper bound, so the caller must say which
    it wants:

    * ``HALF_OPEN`` -- the API excludes ``final``, so a window covering days
      d0..d1 inclusive is emitted as ``(d0, d1 + 1 day)``. Adjacent windows tile
      exactly with no double counting.
    * ``CLOSED`` -- the API includes ``final``, so the window is ``(d0, d1)``.

    The label is stable and filename-safe, so a chunk written under it can be
    skipped on a resumed run.
    """
    if kind not in (WindowKind.HALF_OPEN, WindowKind.CLOSED):
        raise ValueError(f"windows() does not handle {kind}")
    if step_days > MAX_WINDOW_DAYS:
        raise ValueError(
            f"step_days={step_days} exceeds the API's {MAX_WINDOW_DAYS}-day cap"
        )

    out = []
    for first, last in _dates(start, end, step_days):
        upper = (
            last + dt.timedelta(days=1)
            if kind is WindowKind.HALF_OPEN
            else last
        )
        if (upper - first).days > MAX_WINDOW_DAYS:
            raise ValueError(
                f"window {first}..{upper} exceeds the {MAX_WINDOW_DAYS}-day cap"
            )
        out.append(
            (
                f"{first.isoformat()}_{last.isoformat()}",
                first.isoformat(),
                upper.isoformat(),
            )
        )
    return out
