"""
Senado Federal Legislative Open Data — API client + raw extractors.

Pure functions (no Prefect). Shared by the one-shot onboarding and, later, the
recurring pipeline (`pipelines/datasets/br_senado_dados_abertos/utils.py` will
import from here / this will move there).

API: https://legis.senado.leg.br/dadosabertos  — PUBLIC, no auth.
JSON is returned when `Accept: application/json` is sent (XML is the default).
The service intermittently returns an empty body under rapid calls, so every
request retries with backoff.
"""

from __future__ import annotations

import time
from typing import Any

import requests

BASE = "https://legis.senado.leg.br/dadosabertos"
HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
}


def get_json(
    path: str,
    params: dict | None = None,
    retries: int = 6,
    backoff: float = 1.5,
    timeout: int = 120,
) -> Any:
    """GET a dados-abertos endpoint and parse JSON, retrying on empty/error.

    Returns the parsed JSON (dict or list), or None only when the server
    persistently answers 200 with an empty body (some list endpoints are
    legitimately empty). A persistent transport error or non-200 status is
    raised, so a genuine failure is never silently read as "empty".
    """
    url = f"{BASE}{path}"
    last_exc: Exception | None = None
    last_status: int | None = None
    for attempt in range(retries):
        try:
            r = requests.get(
                url, params=params, headers=HEADERS, timeout=timeout
            )
            last_status = r.status_code
            if r.status_code == 200 and r.content and r.text.strip():
                return r.json()
            # 200-but-empty, or non-200 → retry
        except requests.RequestException as exc:
            last_exc = exc
        time.sleep(backoff * (attempt + 1))
    if last_exc is not None:
        raise last_exc
    if last_status is not None and last_status != 200:
        raise RuntimeError(
            f"GET {url} failed: HTTP {last_status} after {retries} attempts"
        )
    return None  # persistent 200-empty: legitimately-empty list endpoint


def get_json_safe(path: str, params: dict | None = None, **kw) -> Any:
    """Like `get_json`, but returns None instead of raising on failure.

    Used for the per-entity fan-out (per senator, per committee), where one
    endpoint persistently erroring must skip that entity, not abort a run that
    iterates thousands of them.
    """
    try:
        return get_json(path, params, **kw)
    except Exception:
        return None


def _as_list(node: Any) -> list:
    """Normalize a possibly-missing / single-object node into a list.

    The Senate envelopes collapse a 1-element array into a lone object and drop
    empty arrays entirely, so `Partido`, `Bloco` etc. may be dict, list or None.
    """
    if node is None:
        return []
    if isinstance(node, list):
        return node
    return [node]


def dig(obj: Any, *keys: str, default: Any = None) -> Any:
    """Safe nested getter: dig(d, 'A', 'B', 'C')."""
    cur = obj
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur
