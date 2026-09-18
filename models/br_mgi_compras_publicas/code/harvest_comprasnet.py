"""Backfill driver for the ComprasNet legado enrichment tables.

Three phases, each resumable at month granularity:

    list      ata4.asp by month window  -> (numprp, uasg, modalidade) per pregao
    crosswalk ata2.asp per pregao       -> prgcod, the key the detail pages take
    detail    FornecedorResultado + termohom per pregao -> oferta + evento rows

Coverage is 2001-06 .. 2024-01; the source stopped receiving pregoes at the Lei
14.133 cutover, so this is a one-shot backfill, not a recurring pipeline.

Scratch data never goes in the repo or under Dropbox. It defaults to
``~/Downloads/br_mgi_compras_publicas_data/comprasnet`` and is deleted once the
tables are published.

Usage
-----
    uv run python models/br_mgi_compras_publicas/code/harvest_comprasnet.py \\
        --start 2019-09 --end 2019-09
    uv run python models/br_mgi_compras_publicas/code/harvest_comprasnet.py \\
        --start 2001-06 --end 2024-01 --workers 8
    uv run python models/br_mgi_compras_publicas/code/harvest_comprasnet.py \\
        --consolidate
"""

from __future__ import annotations

import argparse
import calendar
import datetime as dt
import json
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipelines.datasets.br_mgi_compras_publicas.comprasnet import (  # noqa: E402
    build_session,
    compose_id_compra,
    fetch_page,
    fetch_prgcod,
    list_atas,
    parse_fornecedor_resultado,
    parse_termo_homologacao,
)

logger = logging.getLogger("comprasnet")

DEFAULT_DATA_DIR = (
    Path.home() / "Downloads" / "br_mgi_compras_publicas_data" / "comprasnet"
)
FIRST_MONTH = "2001-06"
LAST_MONTH = "2024-01"
PHASES = ("list", "crosswalk", "detail")

_local = threading.local()


def data_dir() -> Path:
    """The path named by ``COMPRAS_DATA_DIR``, or the default under Downloads."""
    root = os.environ.get("COMPRAS_DATA_DIR")
    base = Path(root) / "comprasnet" if root else DEFAULT_DATA_DIR
    base.mkdir(parents=True, exist_ok=True)
    return base


def months(start: str, end: str) -> list[str]:
    """Inclusive ``YYYY-MM`` range."""
    first = dt.date(int(start[:4]), int(start[5:7]), 1)
    last = dt.date(int(end[:4]), int(end[5:7]), 1)
    out: list[str] = []
    while first <= last:
        out.append(first.strftime("%Y-%m"))
        first = dt.date(
            first.year + (first.month == 12), (first.month % 12) + 1, 1
        )
    return out


def month_bounds(month: str) -> tuple[str, str]:
    year, mon = int(month[:4]), int(month[5:7])
    last_day = calendar.monthrange(year, mon)[1]
    return f"01/{mon:02d}/{year}", f"{last_day:02d}/{mon:02d}/{year}"


def _session():
    """One ComprasNet session per worker thread; ata4.asp needs the ASP cookie."""
    session = getattr(_local, "session", None)
    if session is None:
        session = build_session()
        _local.session = session
    return session


def _phase_dir(phase: str) -> Path:
    path = data_dir() / phase
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_chunk(
    phase: str, month: str, rows: list[dict], expected: int | None = None
) -> None:
    """Write a month's rows plus a sidecar manifest.

    The manifest is what makes resume safe. A chunk file that exists but is
    empty is ambiguous — genuinely no rows, or a run that died mid-write — and
    treating the second case as done is permanent silent loss. Only a chunk with
    a manifest recording the input count it was built from counts as complete.
    """
    target = _phase_dir(phase) / f"{month}.jsonl"
    tmp = target.with_suffix(".jsonl.partial")
    with tmp.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    tmp.replace(target)
    manifest = {
        "month": month,
        "phase": phase,
        "rows": len(rows),
        "expected_inputs": expected,
        "written_at": dt.datetime.now(dt.UTC).isoformat(),
    }
    (_phase_dir(phase) / f"{month}.manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )


def _chunk_done(phase: str, month: str) -> bool:
    return (_phase_dir(phase) / f"{month}.manifest.json").exists()


def _read_chunk(phase: str, month: str) -> list[dict]:
    path = _phase_dir(phase) / f"{month}.jsonl"
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def run_list(month: str, force: bool = False) -> int:
    if _chunk_done("list", month) and not force:
        return len(_read_chunk("list", month))
    start, end = month_bounds(month)
    keys = list_atas(_session(), start, end)
    rows = [
        {
            "month": month,
            "numprp": numprp,
            "uasg": uasg,
            "modalidade": modalidade,
            "id_compra": compose_id_compra(numprp, uasg, modalidade),
        }
        for numprp, uasg, modalidade in keys
    ]
    _write_chunk("list", month, rows)
    logger.info("list %s: %d pregoes", month, len(rows))
    return len(rows)


def run_crosswalk(month: str, workers: int, force: bool = False) -> int:
    if _chunk_done("crosswalk", month) and not force:
        return len(_read_chunk("crosswalk", month))
    listed = _read_chunk("list", month)
    if not listed:
        _write_chunk("crosswalk", month, [], expected=0)
        return 0

    def resolve(row: dict) -> dict | None:
        prgcod = fetch_prgcod(
            _session(), row["numprp"], row["uasg"], row["modalidade"]
        )
        if not prgcod:
            return None
        return {**row, "prgcod": prgcod}

    with ThreadPoolExecutor(workers) as pool:
        resolved = [row for row in pool.map(resolve, listed) if row]
    _write_chunk("crosswalk", month, resolved, expected=len(listed))
    logger.info(
        "crosswalk %s: %d/%d resolved", month, len(resolved), len(listed)
    )
    return len(resolved)


def run_detail(
    month: str, workers: int, force: bool = False
) -> tuple[int, int]:
    if (
        _chunk_done("oferta", month)
        and _chunk_done("evento", month)
        and not force
    ):
        return len(_read_chunk("oferta", month)), len(
            _read_chunk("evento", month)
        )
    crosswalk = _read_chunk("crosswalk", month)
    if not crosswalk:
        _write_chunk("oferta", month, [], expected=0)
        _write_chunk("evento", month, [], expected=0)
        return 0, 0

    def detail(row: dict) -> tuple[list[dict], list[dict]]:
        session = _session()
        id_compra, prgcod = row["id_compra"], row["prgcod"]
        offers: list[dict] = []
        events: list[dict] = []
        page = fetch_page(session, "fornecedor_resultado", prgcod)
        if page:
            offers = parse_fornecedor_resultado(page, id_compra)
        page = fetch_page(session, "termo_homologacao", prgcod)
        if page:
            events = parse_termo_homologacao(page, id_compra)
        return offers, events

    with ThreadPoolExecutor(workers) as pool:
        results = list(pool.map(detail, crosswalk))
    offers = [row for pair in results for row in pair[0]]
    events = [row for pair in results for row in pair[1]]
    _write_chunk("oferta", month, offers, expected=len(crosswalk))
    _write_chunk("evento", month, events, expected=len(crosswalk))
    logger.info(
        "detail %s: %d offers, %d events from %d pregoes",
        month,
        len(offers),
        len(events),
        len(crosswalk),
    )
    return len(offers), len(events)


def consolidate(table: str) -> Path:
    """Concatenate every month chunk of one table into a single JSONL file.

    Built from the planned month list, never from a directory glob: a glob can
    pick up chunks an earlier run wrote under different bounds and double every
    row.
    """
    target = data_dir() / f"{table}.jsonl"
    total = 0
    with target.open("w", encoding="utf-8") as out:
        for month in months(FIRST_MONTH, LAST_MONTH):
            if not _chunk_done(table, month):
                continue
            for row in _read_chunk(table, month):
                out.write(json.dumps(row, ensure_ascii=False) + "\n")
                total += 1
    logger.info("consolidated %s: %d rows -> %s", table, total, target)
    return target


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--start", default=FIRST_MONTH, help="first month, YYYY-MM"
    )
    parser.add_argument(
        "--end", default=LAST_MONTH, help="last month, YYYY-MM"
    )
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument(
        "--phases", nargs="+", default=list(PHASES), choices=PHASES
    )
    parser.add_argument(
        "--force", action="store_true", help="redo completed chunks"
    )
    parser.add_argument("--consolidate", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    if args.consolidate:
        for table in ("oferta", "evento", "crosswalk"):
            consolidate(table)
        return

    started = time.time()
    totals = {"pregoes": 0, "resolved": 0, "oferta": 0, "evento": 0}
    for month in months(args.start, args.end):
        if "list" in args.phases:
            totals["pregoes"] += run_list(month, args.force)
        if "crosswalk" in args.phases:
            totals["resolved"] += run_crosswalk(
                month, args.workers, args.force
            )
        if "detail" in args.phases:
            offers, events = run_detail(month, args.workers, args.force)
            totals["oferta"] += offers
            totals["evento"] += events
    logger.info(
        "done in %.1f min: %s",
        (time.time() - started) / 60,
        json.dumps(totals),
    )


if __name__ == "__main__":
    main()
