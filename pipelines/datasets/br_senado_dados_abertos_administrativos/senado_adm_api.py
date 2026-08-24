"""
Senado Federal Administrative Open Data — API client + raw extractors.

Pure functions (no Prefect). Shared by the one-shot onboarding under
``models/br_senado_dados_abertos_administrativos/code/`` and by the recurring
pipeline, so the extract has exactly one definition.

API: https://adm.senado.gov.br/adm-dadosabertos — PUBLIC, no auth, no key.

Three source behaviours drive the design here; see the dataset's
ONBOARDING_PLAN.md for the evidence behind each.

1. ``404`` means "no rows", not an error. Every per-entity sub-resource returns
   404 when the parent simply has none, so :func:`get_json` maps it to ``[]``.
2. ``/contratacoes/contratos`` **silently returns only ~30% of contracts**
   unless ``statusContratoParam`` is supplied. :func:`fetch_contratacoes` fans
   out over the status enum and unions the result (2,477 → 8,162 rows).
3. The host starts refusing connections above roughly ten concurrent requests,
   so :data:`MAX_WORKERS` is deliberately conservative and every call retries.
"""

from __future__ import annotations

import concurrent.futures as cf
import time
from collections.abc import Callable, Iterable
from typing import Any

import requests

BASE = "https://adm.senado.gov.br/adm-dadosabertos/api/v1"
HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
}

# Measured: ~1.8 s/request; ~6 req/s at 10 threads. Above ~10 concurrent the
# host begins dropping connections, so this is a ceiling, not a target.
MAX_WORKERS = 8

# Statuses that must be requested explicitly — the bare call omits ENCERRADO.
CONTRATO_STATUSES = ("VIGENTE", "EM_RENOVACAO", "ENCERRADO")

TIPOS_CONTRATACAO = ("contratos", "notas_empenho", "atas_registro_preco")


def get_json(
    path: str,
    params: dict | None = None,
    retries: int = 5,
    backoff: float = 1.5,
    timeout: int = 180,
) -> Any:
    """GET an adm-dadosabertos endpoint and parse JSON.

    Returns ``[]`` for HTTP 404, which this API uses to mean "this parent has no
    rows" rather than "not found". A persistent transport error or any other
    non-200 status raises, so a genuine failure is never silently read as empty.
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
            if r.status_code == 404:
                return []
            if r.status_code == 200 and r.content and r.text.strip():
                return r.json()
        except requests.RequestException as exc:
            last_exc = exc
        time.sleep(backoff * (attempt + 1))
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(
        f"GET {url} failed: HTTP {last_status} after {retries} attempts"
    )


def unwrap(payload: Any) -> list[dict]:
    """Normalize both response envelopes to a list of records.

    Endpoints return either a bare JSON array or ``{statusCode, msg, data[]}``.
    """
    if payload is None:
        return []
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return data
        return [data] if isinstance(data, dict) else []
    return list(payload) if isinstance(payload, list) else []


def fetch(path: str, params: dict | None = None) -> list[dict]:
    """GET and unwrap in one step — the common case for a list endpoint."""
    return unwrap(get_json(path, params))


def fan_out(
    items: Iterable[Any],
    fn: Callable[[Any], Any],
    workers: int = MAX_WORKERS,
) -> list[tuple[Any, Any]]:
    """Map ``fn`` over ``items`` concurrently, preserving (item, result) pairs.

    Used for the per-entity sub-resource crawls. Results come back in completion
    order; callers that need determinism sort afterwards. An item whose call
    raises after all retries yields ``(item, None)`` so one bad parent cannot
    abort a crawl over thousands.
    """
    out: list[tuple[Any, Any]] = []
    with cf.ThreadPoolExecutor(workers) as ex:
        futures = {ex.submit(fn, it): it for it in items}
        for fut in cf.as_completed(futures):
            item = futures[fut]
            try:
                out.append((item, fut.result()))
            except Exception:
                out.append((item, None))
    return out


# --------------------------------------------------------------- contratações


def fetch_contratacoes() -> list[dict]:
    """All contratações, with ``tipo_contratacao`` and ``status`` attached.

    ``contratos`` is fanned out over :data:`CONTRATO_STATUSES` because the bare
    call omits ENCERRADO — 2,477 rows without it, 8,162 with. ``notas_empenho``
    and ``atas_registro_preco`` are complete on the bare call (verified: the
    bare result equals the union over their status enum), so they are fetched
    once and their status is left unset.

    ``id`` is unique only *within* a ``tipo_contratacao`` — 577 ids appear in
    both ``contratos`` and ``notas_empenho`` as different entities — so the
    dedup key here, and the table's primary key, is the pair.
    """
    by_key: dict[tuple[str, int], dict] = {}

    for status in CONTRATO_STATUSES:
        for row in fetch(
            "/contratacoes/contratos", {"statusContratoParam": status}
        ):
            row = dict(row, tipo_contratacao="contratos", status=status)
            by_key.setdefault(("contratos", row["id"]), row)

    for tipo in ("notas_empenho", "atas_registro_preco"):
        for row in fetch(f"/contratacoes/{tipo}"):
            row = dict(row, tipo_contratacao=tipo, status=None)
            by_key.setdefault((tipo, row["id"]), row)

    return list(by_key.values())


def fetch_sub_resource(
    parents: list[dict], sub: str, tipos: Iterable[str] = TIPOS_CONTRATACAO
) -> list[dict]:
    """Crawl ``/contratacoes/{tipo}/{id}/{sub}`` over the given parents.

    Each returned record carries its parent's ``tipo_contratacao`` and
    ``id_contratacao`` so the child is joinable on the composite key. Parents
    whose ``tipo_contratacao`` is not in ``tipos`` are skipped — ``aditivos``
    exists only for ``contratos`` and ``acionamentos`` only for
    ``atas_registro_preco``.
    """
    targets = [p for p in parents if p["tipo_contratacao"] in tipos]

    def one(p: dict) -> list[dict]:
        return unwrap(
            get_json(f"/contratacoes/{p['tipo_contratacao']}/{p['id']}/{sub}")
        )

    rows: list[dict] = []
    for parent, result in fan_out(targets, one):
        for rec in result or []:
            rows.append(
                dict(
                    rec,
                    tipo_contratacao=parent["tipo_contratacao"],
                    id_contratacao=parent["id"],
                )
            )
    return rows


def fetch_pagamento_empenhos(pagamentos: list[dict]) -> list[dict]:
    """Crawl ``/pagamentos/{id}/empenhos`` — genuine per-payment detail.

    Unlike the sibling ``documentos_fiscais`` field, which repeats the whole
    contract's document list on every payment, empenhos differ per payment
    (contract 2280 returns three distinct empenhos across its four payments),
    so this branch is a real fan-out rather than a nested read.
    """

    def one(p: dict) -> list[dict]:
        return unwrap(
            get_json(
                f"/contratacoes/{p['tipo_contratacao']}/{p['id_contratacao']}"
                f"/pagamentos/{p['id']}/empenhos"
            )
        )

    rows: list[dict] = []
    for pag, result in fan_out(pagamentos, one):
        for rec in result or []:
            rows.append(
                dict(
                    rec,
                    tipo_contratacao=pag["tipo_contratacao"],
                    id_contratacao=pag["id_contratacao"],
                    id_pagamento=pag["id"],
                )
            )
    return rows
