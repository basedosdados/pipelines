"""Download raw PNCP consulta-API records to newline-delimited JSON.

The PNCP consulta API has four constraints that shape this downloader:

1. ``tamanhoPagina`` is capped at 500 and floored at 10.
2. A date window may not exceed 365 days (HTTP 422 beyond that).
3. Large result sets make the server fail: a full-year window on a
   high-volume modalidade returns HTTP 500, and pages deep into a large
   result set time out (HTTP 504). The fix is to keep each result set
   small, so windows are split adaptively whenever the server fails.
4. Requests are rate limited (HTTP 429) well below what a naive loop issues,
   and the limit is applied per source IP across all endpoints — two scripts
   running at once throttle each other. The 429 body is HTML, not JSON.

A fifth trap is not a constraint but a semantic one, and it is the reason the
atas entry below points at ``atas/atualizacao``: ``/v1/atas`` filters on the
ata's *vigência* period rather than its publication date, so every window
returns the whole live stock.

Output is one gzipped NDJSON file per (table, window, modalidade) chunk under
``input/<table>/``. A chunk file is written atomically and skipped if it already
exists, which makes the whole download resumable after an interruption.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, timedelta
from pathlib import Path

BASE = "https://pncp.gov.br/api/consulta/v1/"
PAGE_SIZE = 500
USER_AGENT = (
    "Mozilla/5.0 (compatible; BaseDosDados/1.0; +https://basedosdados.org)"
)

DATA_DIR = Path(
    os.environ.get("PNCP_DATA_DIR", Path.home() / "Downloads" / "br_pncp_data")
)
INPUT_DIR = DATA_DIR / "input"

# PNCP publishes from 2021 (Lei 14.133/2021).
START_DATE = date(2021, 1, 1)

# codigoModalidadeContratacao is mandatory on the contratacoes endpoints, so
# every window must be crossed with the full modalidade domain.
MODALIDADES = list(range(1, 15))

ENDPOINTS = {
    "contratacao": {
        "path": "contratacoes/publicacao",
        "date_params": ("dataInicial", "dataFinal"),
        "by_modalidade": True,
        "window_days": 30,
    },
    "contrato": {
        "path": "contratos",
        "date_params": ("dataInicial", "dataFinal"),
        "by_modalidade": False,
        "window_days": 15,
    },
    # Harvested by update date, NOT by /v1/atas. That endpoint filters on the
    # ata's *vigência* period, so it returns the entire live stock for any
    # window: a single day (2025-03-10) returns 365,742 records in 21s, versus
    # 1,551 in 1.5s here. Windowing /v1/atas would duplicate the stock once per
    # window. Deduplicate on numeroControlePNCPAta downstream.
    "ata_registro_preco": {
        "path": "atas/atualizacao",
        "date_params": ("dataInicial", "dataFinal"),
        "by_modalidade": False,
        "window_days": 7,
    },
    "instrumento_cobranca": {
        "path": "instrumentoscobranca/inclusao",
        "date_params": ("dataInicial", "dataFinal"),
        "by_modalidade": False,
        "window_days": 30,
    },
    "plano_contratacao_anual": {
        "path": "pca/atualizacao",
        "date_params": ("dataInicio", "dataFim"),
        "by_modalidade": False,
        "window_days": 30,
    },
}


class Throttle:
    """Simple pacer with adaptive backoff after rate-limit responses."""

    def __init__(self, min_interval: float = 0.35):
        self.base = min_interval
        self.interval = min_interval
        self._last = 0.0

    def wait(self) -> None:
        gap = time.monotonic() - self._last
        if gap < self.interval:
            time.sleep(self.interval - gap)
        self._last = time.monotonic()

    def penalise(self) -> None:
        self.interval = min(self.interval * 1.6, 8.0)

    def relax(self) -> None:
        self.interval = max(self.base, self.interval * 0.95)


THROTTLE = Throttle(float(os.environ.get("PNCP_MIN_INTERVAL", "0.35")))


class ServerOverloadError(Exception):
    """The window is too large for the server to answer. Split it."""


def request(path: str, params: dict, max_tries: int = 6) -> dict:
    url = BASE + path + "?" + urllib.parse.urlencode(params)
    for attempt in range(max_tries):
        THROTTLE.wait()
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": USER_AGENT,
                },
            )
            with urllib.request.urlopen(req, timeout=240) as resp:
                if resp.status == 204:
                    THROTTLE.relax()
                    return {"data": [], "totalRegistros": 0, "totalPaginas": 0}
                payload = json.loads(resp.read().decode("utf-8"))
            THROTTLE.relax()
            return payload
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                THROTTLE.penalise()
                time.sleep(min(60, 5 * (attempt + 1)) + random.uniform(0, 2))
                continue
            if exc.code in (500, 502, 503, 504):
                # Retry a couple of times; a persistent failure means the
                # result set is too large and the caller must split the window.
                if attempt >= 2:
                    raise ServerOverloadError(f"{exc.code} on {url}") from exc
                time.sleep(4 * (attempt + 1))
                continue
            if exc.code == 422:
                raise ServerOverloadError(f"422 on {url}") from exc
            raise RuntimeError(
                f"HTTP {exc.code} on {url}: {exc.read()[:200]!r}"
            ) from exc
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            if attempt >= max_tries - 2:
                raise ServerOverloadError(
                    f"transport failure on {url}"
                ) from None
            time.sleep(4 * (attempt + 1))
    raise ServerOverloadError(f"exhausted retries on {url}")


def fetch_window(path: str, params: dict) -> list[dict]:
    """Page through one window, raising ServerOverloadError if it is too large."""
    records: list[dict] = []
    page = 1
    while True:
        payload = request(
            path, {**params, "pagina": page, "tamanhoPagina": PAGE_SIZE}
        )
        batch = payload.get("data") or []
        records.extend(batch)
        total_pages = payload.get("totalPaginas") or 0
        if page >= total_pages or not batch:
            break
        page += 1
    return records


def windows(start: date, end: date, days: int):
    cur = start
    while cur <= end:
        stop = min(cur + timedelta(days=days - 1), end)
        yield cur, stop
        cur = stop + timedelta(days=1)


def collect(
    path: str, date_params: tuple[str, str], lo: date, hi: date, extra: dict
) -> list[dict]:
    """Fetch [lo, hi], halving the window whenever the server buckles."""
    p_from, p_to = date_params
    params = {
        **extra,
        p_from: lo.strftime("%Y%m%d"),
        p_to: hi.strftime("%Y%m%d"),
    }
    try:
        return fetch_window(path, params)
    except ServerOverloadError:
        if lo == hi:
            # A single day the server cannot serve. Record and move on rather
            # than aborting the whole run; the gap is reported at the end.
            print(f"      !! unrecoverable single day {lo}", flush=True)
            return []
        mid = lo + (hi - lo) // 2
        print(f"      .. splitting {lo}..{hi}", flush=True)
        return collect(path, date_params, lo, mid, extra) + collect(
            path, date_params, mid + timedelta(days=1), hi, extra
        )


def write_chunk(target: Path, records: list[dict]) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".partial")
    with gzip.open(tmp, "wt", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    tmp.replace(target)


def run_table(table: str, start: date, end: date) -> None:
    spec = ENDPOINTS[table]
    out_dir = INPUT_DIR / table
    combos = (
        [{"codigoModalidadeContratacao": m} for m in MODALIDADES]
        if spec["by_modalidade"]
        else [{}]
    )

    total = 0
    for lo, hi in windows(start, end, spec["window_days"]):
        for extra in combos:
            tag = f"{lo:%Y%m%d}_{hi:%Y%m%d}"
            if extra:
                tag += f"_m{extra['codigoModalidadeContratacao']:02d}"
            target = out_dir / f"{tag}.jsonl.gz"
            if target.exists():
                continue
            records = collect(spec["path"], spec["date_params"], lo, hi, extra)
            write_chunk(target, records)
            total += len(records)
            print(
                f"  {table} {tag}: {len(records):>7,} rows (run total {total:,})",
                flush=True,
            )
    print(f"== {table}: {total:,} new rows this run", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--tables", nargs="*", default=list(ENDPOINTS), choices=list(ENDPOINTS)
    )
    ap.add_argument(
        "--start", type=lambda s: date.fromisoformat(s), default=START_DATE
    )
    ap.add_argument(
        "--end", type=lambda s: date.fromisoformat(s), default=date.today()
    )
    args = ap.parse_args()

    for table in args.tables:
        print(f"### {table} {args.start} .. {args.end}", flush=True)
        run_table(table, args.start, args.end)
    return 0


if __name__ == "__main__":
    sys.exit(main())
