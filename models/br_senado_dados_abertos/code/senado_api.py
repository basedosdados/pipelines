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

    Returns the parsed JSON (dict or list), or None if the server keeps
    returning an empty body (some list endpoints legitimately return empty).
    """
    url = f"{BASE}{path}"
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            r = requests.get(
                url, params=params, headers=HEADERS, timeout=timeout
            )
            if r.status_code == 200 and r.content and r.text.strip():
                return r.json()
            # 200-but-empty, or 5xx → retry
        except requests.RequestException as exc:
            last_exc = exc
        time.sleep(backoff * (attempt + 1))
    if last_exc is not None:
        raise last_exc
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
