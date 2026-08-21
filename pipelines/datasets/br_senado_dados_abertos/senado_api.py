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
import xml.etree.ElementTree as ET
from typing import Any
from xml.etree.ElementTree import ParseError

import requests

# defusedxml is not a dependency of the worker image; these responses come from a
# known government host over TLS and are parsed with the stdlib parser, which has
# entity expansion disabled by default in Python 3.12 for `fromstring`.

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


def get_xml_records(
    path: str,
    record_tag: str,
    retries: int = 6,
    backoff: float = 1.5,
    timeout: int = 120,
) -> list[dict]:
    """GET an endpoint as XML and return every `record_tag` element as a dict.

    Some dados-abertos endpoints cannot be read as JSON. `senador/lista/
    legislatura/{leg}` lost its `ListaParlamentarLegislatura` envelope some time
    between 2026-08-13 and 2026-08-20: the XML became a rootless concatenation of
    `<Parlamentar>` elements, and because a JSON object cannot carry the same key
    245 times, the JSON serialization collapses the whole roster to a **single**
    record. Reading that endpoint as JSON is therefore silently lossy, not merely
    broken, so it is read as XML here.

    A rootless body is wrapped before parsing, and a body that still carries its
    envelope parses unchanged — so this keeps working if the Senate restores it.

    Args:
        path: Endpoint path below `BASE`.
        record_tag: Element tag to collect (e.g. ``"Parlamentar"``).
        retries: Attempts before giving up.
        backoff: Linear backoff base, in seconds.
        timeout: Per-request timeout, in seconds.

    Returns:
        One dict per record, nested exactly as the JSON envelope would nest it.

    Raises:
        RuntimeError: On a persistent non-200, or a body that will not parse.
    """
    url = f"{BASE}{path}"
    headers = {**HEADERS, "Accept": "application/xml"}
    last_exc: Exception | None = None
    last_status: int | None = None
    saw_empty_200 = False
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            last_status = r.status_code
            if r.status_code == 200:
                body = r.text.strip()
                if body:
                    return _parse_xml_records(body, record_tag)
                saw_empty_200 = True
        except requests.RequestException as exc:
            last_exc = exc
        time.sleep(backoff * (attempt + 1))
    if saw_empty_200:
        # Mirrors `get_json`: a persistent 200 with an empty body is a
        # legitimately-empty list endpoint, not a failure. The roster endpoint
        # answers exactly this for legislatures that predate the API's coverage
        # (36 returns 200 with 0 bytes; 40 returns ~92 KB). Raising here aborted
        # the whole run on the first such legislature.
        return []
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(
        f"GET {url} (xml) failed: HTTP {last_status} after {retries} attempts"
    )


def _parse_xml_records(text: str, record_tag: str) -> list[dict]:
    """Parse `record_tag` elements out of an XML body, rootless or not.

    Args:
        text: The raw XML body.
        record_tag: Element tag to collect.

    Returns:
        One dict per matching element, found at any depth.

    Raises:
        RuntimeError: If the body will not parse even when wrapped.
    """
    body = text.strip()
    if body.startswith("<?xml"):
        body = body.split("?>", 1)[-1].lstrip()
    try:
        root = ET.fromstring(body)
        # An enveloped response parses as its own root; a rootless concatenation
        # of N records parses only after wrapping, handled below.
        found = [root] if root.tag == record_tag else root.iter(record_tag)
        return [_xml_to_dict(e) for e in found]
    except ParseError:
        pass
    try:
        root = ET.fromstring(f"<_bdwrap>{body}</_bdwrap>")
    except ParseError as exc:
        raise RuntimeError(
            f"could not parse XML for <{record_tag}> records: {exc}"
        ) from exc
    return [_xml_to_dict(e) for e in root.iter(record_tag)]


def _xml_to_dict(elem: ET.Element) -> Any:
    """Convert an XML element to the shape the JSON envelope would have.

    Leaf elements become their stripped text (or None when empty). Repeated
    sibling tags collapse into a list, matching `_as_list`'s expectations.

    Args:
        elem: The element to convert.

    Returns:
        A dict for a branch element, or str/None for a leaf.
    """
    children = list(elem)
    if not children:
        text = (elem.text or "").strip()
        return text or None
    out: dict[str, Any] = {}
    for child in children:
        value = _xml_to_dict(child)
        if child.tag in out:
            existing = out[child.tag]
            if isinstance(existing, list):
                existing.append(value)
            else:
                out[child.tag] = [existing, value]
        else:
            out[child.tag] = value
    return out
