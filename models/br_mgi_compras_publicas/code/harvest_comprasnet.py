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
from collections.abc import Iterator
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
from pipelines.datasets.br_mgi_compras_publicas.utils import (  # noqa: E402
    COERCERS,
    PartitionedStringWriter,
    load_architecture,
)

logger = logging.getLogger("comprasnet")

DEFAULT_DATA_DIR = (
    Path.home() / "Downloads" / "br_mgi_compras_publicas_data" / "comprasnet"
)
FIRST_MONTH = "2001-06"
LAST_MONTH = "2024-01"
PHASES = ("list", "crosswalk", "detail")
#: Chunk directory names are the BigQuery table names, so a chunk dir maps to
#: its architecture CSV without a lookup.
TABLE_OFERTA = "pregao_item_oferta"
TABLE_EVENTO = "pregao_item_evento"

_local = threading.local()


class Progress:
    """Log every ``every`` completions, so a multi-day run is observable.

    Without this a phase is silent until the month ends, and a stalled run is
    indistinguishable from a slow one.
    """

    def __init__(self, label: str, total: int, every: int = 500) -> None:
        self.label, self.total, self.every = label, total, every
        self.done = 0
        self.started = time.time()
        self.lock = threading.Lock()

    def tick(self) -> None:
        with self.lock:
            self.done += 1
            if self.done % self.every and self.done != self.total:
                return
            elapsed = time.time() - self.started
            rate = self.done / elapsed if elapsed else 0.0
            remaining = (self.total - self.done) / rate if rate else 0.0
            logger.info(
                "%s %d/%d (%.1f req/s, ~%.0f min left)",
                self.label,
                self.done,
                self.total,
                rate,
                remaining / 60,
            )


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
    """Whether a month's chunk is complete *and* still on disk.

    The manifest alone is not enough. The 2001-2024 harvest finished on
    2026-09-27 reporting every month written, but the twenty chunks from
    2022-06 on were gone, while their manifests — four orders of magnitude
    smaller — survived. Resume then counted those months as done, and the
    parquet pass read zero rows from each: 1.9M of 18.7M offers, 12% of the
    events, absent with no error anywhere. A month counts as done only if the
    rows it claims are still readable.
    """
    manifest = _phase_dir(phase) / f"{month}.manifest.json"
    chunk = _phase_dir(phase) / f"{month}.jsonl"
    if not manifest.exists() or not chunk.exists():
        return False
    try:
        rows = (
            json.loads(manifest.read_text(encoding="utf-8")).get("rows") or 0
        )
    except (OSError, ValueError):
        # An unreadable manifest is not evidence of a complete month.
        return False
    # A month with genuinely no rows writes a 0-byte chunk, so an empty file is
    # only evidence of loss when the manifest claims rows.
    return rows == 0 or chunk.stat().st_size > 0


def _require_complete(table: str) -> None:
    """Refuse a full-table pass that would quietly write short output.

    ``consolidate`` and ``to_parquet`` skip months that are not done, which is
    right while the harvest is still filling them in and wrong once it claims
    to be finished: a missing month then produces a table that is short by
    however much it held, with nothing in the logs to say so. Fail here instead,
    naming the months, so the harvest can be re-run to refill them.
    """
    planned = months(FIRST_MONTH, LAST_MONTH)
    missing = [month for month in planned if not _chunk_done(table, month)]
    if missing:
        raise SystemExit(
            f"{table}: {len(missing)} of {len(planned)} months are missing or "
            "incomplete, so this pass would write short output. Re-run the "
            "harvest to refill them, then repeat this pass. Missing: "
            + ", ".join(missing)
        )


def _read_chunk(phase: str, month: str) -> list[dict]:
    path = _phase_dir(phase) / f"{month}.jsonl"
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def _iter_chunk(phase: str, month: str) -> Iterator[dict]:
    """``_read_chunk`` without holding the month in memory.

    The resume path wants a list to count; a full-table pass wants neither the
    month nor the table resident, so it takes rows one at a time.
    """
    path = _phase_dir(phase) / f"{month}.jsonl"
    if not path.exists():
        return
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


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

    progress = Progress(f"crosswalk {month}", len(listed))

    def resolve(row: dict) -> dict | None:
        # Nothing raised from inside a worker may escape: pool.map re-raises on
        # the first failed future and ends the whole run. A 5-day harvest died
        # at hour 7 to one dropped connection because this was missing.
        try:
            prgcod = fetch_prgcod(
                _session(), row["numprp"], row["uasg"], row["modalidade"]
            )
        except Exception:
            logger.warning(
                "crosswalk %s/%s failed",
                row["uasg"],
                row["numprp"],
                exc_info=False,
            )
            prgcod = None
        progress.tick()
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
        _chunk_done(TABLE_OFERTA, month)
        and _chunk_done(TABLE_EVENTO, month)
        and not force
    ):
        return len(_read_chunk(TABLE_OFERTA, month)), len(
            _read_chunk(TABLE_EVENTO, month)
        )
    crosswalk = _read_chunk("crosswalk", month)
    if not crosswalk:
        _write_chunk(TABLE_OFERTA, month, [], expected=0)
        _write_chunk(TABLE_EVENTO, month, [], expected=0)
        return 0, 0

    progress = Progress(f"detail {month}", len(crosswalk))

    def detail(row: dict) -> tuple[list[dict], list[dict]]:
        # Same contract as the crosswalk worker: nothing may escape. fetch_page
        # only catches requests.RequestException, so a DNS outage, an OSError
        # from a disturbed venv, or a parse failure on malformed HTML would all
        # reach pool.map and end the run. Losing one pregao is cheap; losing the
        # month in flight and the process is not.
        session = _session()
        id_compra, prgcod = row["id_compra"], row["prgcod"]
        offers: list[dict] = []
        events: list[dict] = []
        try:
            page = fetch_page(session, "fornecedor_resultado", prgcod)
            if page:
                offers = parse_fornecedor_resultado(page, id_compra)
            page = fetch_page(session, "termo_homologacao", prgcod)
            if page:
                events = parse_termo_homologacao(page, id_compra)
        except Exception:
            logger.warning("detail prgcod=%s failed", prgcod, exc_info=False)
            offers, events = [], []
        progress.tick()
        return offers, events

    with ThreadPoolExecutor(workers) as pool:
        results = list(pool.map(detail, crosswalk))
    offers = [row for pair in results for row in pair[0]]
    events = [row for pair in results for row in pair[1]]
    _write_chunk(TABLE_OFERTA, month, offers, expected=len(crosswalk))
    _write_chunk(TABLE_EVENTO, month, events, expected=len(crosswalk))
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
    _require_complete(table)
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


def to_parquet(table: str) -> int:
    """Write the month chunks of one table as hive-partitioned parquet.

    Output lands under ``<data root>/output/<table>/ano=<year>/data.parquet``,
    the layout upload.py expects. Every column is written as STRING: staging is
    all-STRING by house convention, the dbt model safe_casts each column back,
    and a typed staging table would collide with any later overwrite.
    """
    _require_complete(table)
    # `ano` is encoded in the directory name, so it must not also be a column
    # inside the file: pyarrow refuses to merge a string column against the
    # int32 it infers from the hive key, and upload.py's header helper makes the
    # same assumption when it seeds the 0-row schema blob.
    columns = [c for c in load_architecture(table) if c.name != "ano"]
    root = data_dir().parent / "output" / table
    # Rows are streamed into one open writer per year rather than grouped in a
    # dict first. Buffering the table cost ~19M dicts, which does not fit in
    # 16 GB, and the process took the machine down instead of raising.
    #
    # The years cannot be closed as the months advance: `ano` is the pregao's
    # process year, taken from id_compra, not the month the ata was published,
    # so the 2013-11 chunk carries 2011 and 2012 rows. Every year stays open
    # until the pass ends.
    with PartitionedStringWriter(root, columns, "ano") as writer:
        for month in months(FIRST_MONTH, LAST_MONTH):
            if not _chunk_done(table, month):
                continue
            for row in _iter_chunk(table, month):
                # The scraper emits every field as text; the writer casts
                # through each column's real type before stringifying, so
                # coerce first with the dataset's own rules rather than a
                # parallel implementation.
                typed = {
                    column.name: COERCERS[column.bigquery_type](
                        row.get(column.name) or None
                    )
                    for column in columns
                }
                writer.append(row["ano"], typed)
        total = sum(writer.counts.values())
        years = len(writer.counts)
    logger.info(
        "parquet %s: %d rows across %d years -> %s", table, total, years, root
    )
    return total


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
    parser.add_argument("--to-parquet", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    if args.consolidate:
        for table in (TABLE_OFERTA, TABLE_EVENTO, "crosswalk"):
            consolidate(table)
        return

    if args.to_parquet:
        for table in (TABLE_OFERTA, TABLE_EVENTO):
            to_parquet(table)
        return

    started = time.time()
    totals = {"pregoes": 0, "resolved": 0, TABLE_OFERTA: 0, TABLE_EVENTO: 0}
    for month in months(args.start, args.end):
        if "list" in args.phases:
            totals["pregoes"] += run_list(month, args.force)
        if "crosswalk" in args.phases:
            totals["resolved"] += run_crosswalk(
                month, args.workers, args.force
            )
        if "detail" in args.phases:
            offers, events = run_detail(month, args.workers, args.force)
            totals[TABLE_OFERTA] += offers
            totals[TABLE_EVENTO] += events
    logger.info(
        "done in %.1f min: %s",
        (time.time() - started) / 60,
        json.dumps(totals),
    )


if __name__ == "__main__":
    main()
