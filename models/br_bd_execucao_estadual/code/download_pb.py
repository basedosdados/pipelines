"""Download Paraíba execution and procurement from the CGE-PB REST API.

PB is the cheapest source in this dataset and the only one besides SC with a natively
keyed empenho -> liquidação -> pagamento chain. `ano` and `mes` are required on every
despesas endpoint, which makes the harvest self-chunking and an incremental refresh
free; `/compras/*` takes `ano` alone.

Output is one NDJSON file per (endpoint, period), written as the API returns it. The
flattening of the two JSON-string columns and the cast to all-STRING parquet happen in
`clean_pb.py`, so `input/` stays a faithful mirror.

**Every period is checked against `paginacao.total` before it is kept**, the same
control-total discipline as SC: a paginated harvest that drops a page mid-way looks
exactly like a smaller month. An empty chunk is the failure that becomes permanent
silent loss once a resume marker is written over it.

Note that 2014 and earlier return **HTTP 400**, not an empty result. A reader that
treats any non-200 as "no data here" would record those years as genuine gaps.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from constants import (
    BROWSER_UA,
    INPUT_DIR,
    PB_API,
    PB_COMPRAS_ENDPOINTS,
    PB_DESPESA_ENDPOINTS,
    PB_ENVELOPE_PAGE,
    PB_ENVELOPE_ROWS,
    PB_FIRST_YEAR,
    PB_LAST_YEAR,
    PB_PER_PAGE,
)

PB_INPUT = INPUT_DIR / "pb"


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": BROWSER_UA, "Accept": "application/json"})
    return s


def _get(session: requests.Session, url: str, params: dict, retries: int = 4):
    """One page. Returns (rows, total) or (None, None) when the API rejects the period."""
    for attempt in range(retries):
        try:
            r = session.get(url, params=params, timeout=300)
            if r.status_code in (400, 404):
                # A period the API will not serve, which is NOT the same as an empty
                # one -- and there are two distinct reasons for it. 2014 and earlier
                # return **400** (before coverage begins). A month that has not
                # happened yet returns **404**: the sweep runs to December of the
                # current exercise, so every month after today is a 404 and an
                # unhandled one crashes the run at the first future month.
                return None, None
            r.raise_for_status()
            body = r.json()
            if not isinstance(body, dict):
                raise ValueError(f"unexpected envelope: {body!r:.120}")
            rows = body.get(PB_ENVELOPE_ROWS) or []
            total = (body.get(PB_ENVELOPE_PAGE) or {}).get("total")
            return rows, total
        except Exception as exc:
            if attempt == retries - 1:
                raise
            print(
                f"      {type(exc).__name__}, retry {attempt + 1}", flush=True
            )
            time.sleep(8 * (attempt + 1))
    return None, None


def harvest(
    session: requests.Session,
    endpoint: str,
    table: str,
    params: dict,
    label: str,
) -> tuple[str, int]:
    dest = PB_INPUT / table / f"{table}_{label}.ndjson"
    meta = dest.with_suffix(".json")
    dest.parent.mkdir(parents=True, exist_ok=True)

    first, total = _get(
        session, f"{PB_API}/{endpoint}", dict(params, page=1, per_page=1)
    )
    if first is None:
        return "rejected", 0
    if not total:
        return "empty", 0

    if dest.exists() and meta.exists():
        recorded = json.loads(meta.read_text())
        if recorded.get("rows") == total:
            return "skip", total

    rows: list[dict] = []
    page = 1
    while True:
        got, _ = _get(
            session,
            f"{PB_API}/{endpoint}",
            dict(params, page=page, per_page=PB_PER_PAGE),
        )
        if got is None:
            raise SystemExit(f"{label}: page {page} rejected mid-harvest")
        if not got:
            # An empty page before the total is reached means pages were lost, not that
            # the data ended. Refuse rather than write a short file a resume would
            # then treat as complete.
            if len(rows) < total:
                raise SystemExit(
                    f"{label}: empty page {page} after {len(rows):,} of {total:,} rows"
                )
            break
        rows.extend(got)
        if len(rows) >= total:
            break
        page += 1
        time.sleep(0.15)

    if len(rows) != total:
        raise SystemExit(
            f"{label}: harvested {len(rows):,} rows but the API reports {total:,}"
        )

    tmp = dest.with_suffix(".part")
    with tmp.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    tmp.replace(dest)
    meta.write_text(
        json.dumps({"endpoint": endpoint, "label": label, "rows": len(rows)})
    )
    return "ok", len(rows)


def main(years: set[int] | None = None, only: str | None = None) -> None:
    session = _session()
    years = years or set(range(PB_FIRST_YEAR, PB_LAST_YEAR + 1))
    totals: dict[str, int] = {}

    for endpoint, table in PB_DESPESA_ENDPOINTS.items():
        if only and only != endpoint:
            continue
        subtotal = 0
        for year in sorted(years):
            for month in range(1, 13):
                label = f"{year}{month:02d}"
                status, n = harvest(
                    session,
                    f"despesas/{endpoint}",
                    table,
                    {"ano": year, "mes": month},
                    label,
                )
                subtotal += n
                if status not in ("empty", "rejected"):
                    print(
                        f"  {table:<18} {label} {status:<6} {n:>9,}",
                        flush=True,
                    )
                time.sleep(0.2)
        totals[table] = subtotal
        print(f"== {table}: {subtotal:,}", flush=True)

    for endpoint, table in PB_COMPRAS_ENDPOINTS.items():
        if only and only != endpoint:
            continue
        subtotal = 0
        for year in sorted(years):
            status, n = harvest(
                session, f"compras/{endpoint}", table, {"ano": year}, str(year)
            )
            subtotal += n
            if status not in ("empty", "rejected"):
                print(
                    f"  {table:<18} {year}   {status:<6} {n:>9,}", flush=True
                )
            time.sleep(0.2)
        totals[table] = subtotal
        print(f"== {table}: {subtotal:,}", flush=True)

    for table, n in totals.items():
        print(f"{table:<20} {n:>12,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, action="append")
    parser.add_argument(
        "--only", help="a single endpoint name, e.g. notas_empenho"
    )
    args = parser.parse_args()
    main(years=set(args.year) if args.year else None, only=args.only)
