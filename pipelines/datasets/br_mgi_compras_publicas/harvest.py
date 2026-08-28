"""Job planning and execution for the br_mgi_compras_publicas harvest.

A *job* is the smallest independently resumable unit of work: one API query (or
one block of pages of it) whose cleaned output is written atomically to a single
parquet chunk. A run that dies halfway re-plans the same jobs and skips every
chunk already on disk, so restarting is free.
"""

from __future__ import annotations

import datetime as dt
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from pipelines.datasets.br_mgi_compras_publicas.api import (
    build_session,
    fetch_page,
    windows,
)
from pipelines.datasets.br_mgi_compras_publicas.constants import (
    WindowKind,
    constants,
)
from pipelines.datasets.br_mgi_compras_publicas.utils import (
    TABLE_SPECS,
    Column,
    TableSpec,
    load_architecture,
    write_chunk,
)

logger = logging.getLogger(__name__)

PAGE_SIZE = constants.PAGE_SIZE.value
#: Pages per job when an endpoint cannot be split by date. Small enough that
#: the work fans out across workers, large enough that chunk files stay chunky.
#: Endpoints with no date filter are otherwise a single sequential stream --
#: licitacao_item modalidade 1 alone is ~7,000 pages at ~11s each, which is
#: about 21 hours in one thread.
PAGES_PER_BLOCK = 50


@dataclass(frozen=True)
class Job:
    table: str
    chunk_id: str
    params: dict[str, Any]
    #: year to stamp on rows whose payload carries no date of its own
    year_fallback: int | None = None
    #: 1-based page range, inclusive, for endpoints chunked by page block
    page_from: int = 1
    page_to: int | None = None

    def chunk_path(self, output_dir: Path) -> Path:
        return output_dir / "_chunks" / self.table / f"{self.chunk_id}.parquet"


def _year_range(spec: TableSpec, today: dt.date) -> tuple[int, int]:
    first = spec.first_year or 2000
    last = spec.last_year or today.year
    return first, last


def count_pages(
    session: requests.Session, spec: TableSpec, params: dict[str, Any]
) -> int:
    """Pages this query spans, from the envelope's own totals."""
    envelope = fetch_page(
        session, spec.path, {**params, "pagina": 1, "tamanhoPagina": PAGE_SIZE}
    )
    total = int(envelope.get("totalRegistros") or 0)
    if total <= 0:
        return 0
    return -(-total // PAGE_SIZE)  # ceil


def _page_range_jobs(
    session: requests.Session,
    spec: TableSpec,
    chunk_id: str,
    params: dict[str, Any],
) -> list[Job]:
    """Split one paginated query into independent page-range jobs."""
    pages = count_pages(session, spec, params)
    if pages == 0:
        return [
            Job(
                spec.table,
                f"{chunk_id}__p00001",
                params,
                page_from=1,
                page_to=1,
            )
        ]
    jobs = []
    for start in range(1, pages + 1, PAGES_PER_BLOCK):
        stop = min(start + PAGES_PER_BLOCK - 1, pages)
        jobs.append(
            Job(
                spec.table,
                f"{chunk_id}__p{start:05d}",
                params,
                page_from=start,
                page_to=stop,
            )
        )
    return jobs


def plan_jobs(
    spec: TableSpec,
    *,
    today: dt.date | None = None,
    orgaos: list[str] | None = None,
    since: dt.date | None = None,
    session: requests.Session | None = None,
) -> list[Job]:
    """Enumerate every job needed to cover `spec`.

    `since` narrows date-windowed tables to a trailing window, which is what the
    recurring pipeline passes; the onboarding backfill leaves it unset.
    """
    today = today or dt.date.today()
    first_year, last_year = _year_range(spec, today)
    jobs: list[Job] = []

    if spec.window in (WindowKind.HALF_OPEN, WindowKind.CLOSED):
        assert spec.date_params
        lo = (
            max(dt.date(first_year, 1, 1), since)
            if since
            else dt.date(first_year, 1, 1)
        )
        hi = dt.date(last_year, 12, 31)
        if lo > hi:
            return []
        lo_param, hi_param = spec.date_params
        for label, start, end in windows(spec.window, lo, hi, spec.step_days):
            base = {**spec.params, lo_param: start, hi_param: end}
            if spec.modalidades:
                for modalidade in spec.modalidades:
                    jobs.append(
                        Job(
                            spec.table,
                            f"m{modalidade}__{label}",
                            {**base, spec.modalidade_param: modalidade},
                        )
                    )
            else:
                jobs.append(Job(spec.table, label, base))

    elif spec.window is WindowKind.YEAR:
        assert spec.year_param
        for year in range(first_year, last_year + 1):
            if since and year < since.year:
                continue
            jobs.append(
                Job(
                    spec.table,
                    f"y{year}",
                    {**spec.params, spec.year_param: year},
                    year_fallback=year,
                )
            )

    elif spec.window is WindowKind.MODALIDADE:
        # No date filter exists, so the only way to parallelise is by page range.
        planner = session or build_session()
        for modalidade in spec.modalidades:
            params = {**spec.params, spec.modalidade_param: modalidade}
            jobs.extend(
                _page_range_jobs(planner, spec, f"m{modalidade}", params)
            )

    elif spec.window is WindowKind.ORGAO:
        assert spec.date_params and orgaos is not None
        lo_param, hi_param = spec.date_params
        for orgao in orgaos:
            for year in range(first_year, last_year + 1):
                if since and year < since.year:
                    continue
                jobs.append(
                    Job(
                        spec.table,
                        f"o{orgao}__y{year}",
                        {
                            **spec.params,
                            "codigoOrgao": orgao,
                            lo_param: f"{year}-01-01",
                            hi_param: f"{year}-12-31",
                        },
                        year_fallback=year,
                    )
                )

    elif spec.window is WindowKind.SNAPSHOT:
        planner = session or build_session()
        if spec.snapshot_param:
            for value in spec.snapshot_values:
                params = {**spec.params, spec.snapshot_param: value}
                jobs.extend(
                    _page_range_jobs(planner, spec, f"s{value}", params)
                )
        else:
            jobs.extend(
                _page_range_jobs(planner, spec, "all", dict(spec.params))
            )

    else:  # pragma: no cover -- WindowKind is exhaustive
        raise ValueError(f"unhandled window kind {spec.window}")

    return jobs


def run_job(
    session: requests.Session,
    spec: TableSpec,
    job: Job,
    columns: list[Column],
    output_dir: Path,
    *,
    extraction_date: dt.date | None = None,
) -> int:
    """Execute one job, writing exactly one chunk. Returns rows written.

    A job that returns nothing still writes an empty chunk, so a resumed run
    does not re-query windows already known to be empty -- which, for the
    per-orgao contrato loop, is about 94% of them.
    """
    from pipelines.datasets.br_mgi_compras_publicas.utils import clean_records

    rows: list[dict[str, Any]] = []
    page = job.page_from
    while True:
        envelope = fetch_page(
            session,
            spec.path,
            {**job.params, "pagina": page, "tamanhoPagina": PAGE_SIZE},
        )
        page_rows = envelope.get("resultado") or []
        rows.extend(page_rows)
        if not page_rows or envelope.get("paginasRestantes", 0) <= 0:
            break
        if job.page_to is not None and page >= job.page_to:
            break
        page += 1

    cleaned = clean_records(
        spec,
        rows,
        columns,
        year_fallback=job.year_fallback,
        extraction_date=extraction_date,
    )
    return write_chunk(cleaned, columns, job.chunk_path(output_dir))


def pending_jobs(jobs: list[Job], output_dir: Path) -> list[Job]:
    """Drop jobs whose chunk is already on disk."""
    return [job for job in jobs if not job.chunk_path(output_dir).exists()]


def harvest_table(
    table: str,
    output_dir: Path,
    *,
    max_workers: int | None = None,
    orgaos: list[str] | None = None,
    since: dt.date | None = None,
    today: dt.date | None = None,
    extraction_date: dt.date | None = None,
    progress_every: int = 200,
) -> dict[str, int]:
    """Harvest one table into resumable parquet chunks."""
    spec = TABLE_SPECS[table]
    columns = load_architecture(table)
    planner = build_session()
    planned = plan_jobs(
        spec, today=today, orgaos=orgaos, since=since, session=planner
    )
    todo = pending_jobs(planned, output_dir)
    workers = max_workers or constants.MAX_WORKERS.value
    logger.info(
        "%s: %d jobs planned, %d pending (%d already done)",
        table,
        len(planned),
        len(todo),
        len(planned) - len(todo),
    )

    rows = 0
    failures = 0
    session_pool = [build_session() for _ in range(workers)]

    def work(index_job: tuple[int, Job]) -> int:
        index, job = index_job
        session = session_pool[index % workers]
        return run_job(
            session,
            spec,
            job,
            columns,
            output_dir,
            extraction_date=extraction_date,
        )

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(work, (i, job)): job for i, job in enumerate(todo)
        }
        for done, future in enumerate(as_completed(futures), start=1):
            job = futures[future]
            try:
                rows += future.result()
            except Exception:
                failures += 1
                logger.exception("%s: job %s failed", table, job.chunk_id)
            if done % progress_every == 0:
                logger.info(
                    "%s: %d/%d jobs, %d rows", table, done, len(todo), rows
                )

    logger.info("%s: done, %d rows, %d failed jobs", table, rows, failures)
    return {
        "planned": len(planned),
        "ran": len(todo),
        "rows": rows,
        "failures": failures,
    }


# --------------------------------------------------------------------------
# Orgao discovery for the contrato tables
# --------------------------------------------------------------------------

#: Densest year for contract starts, used as the single probe year.
CONTRATO_PROBE_YEAR = 2025


def orgaos_from_chunks(output_dir: Path) -> set[str]:
    """Orgao codes already visible in harvested chunks.

    Every contract originates in some procurement, and the harvest covers all
    procurement from 1997, so this is close to the full set of orgaos that can
    hold a contract. It is not provably complete -- a carona contract is signed
    by an orgao other than the one that ran the ata -- which is why the probe
    below exists as a second source.
    """
    import pyarrow.parquet as pq

    sources = {
        "contratacao": "codigo_orgao",
        "compra_sem_licitacao": "codigo_orgao",
        "compra_sem_licitacao_item": "codigo_orgao",
        "ata_registro_preco": "codigo_orgao",
        "licitacao_pregao": "codigo_orgao",
    }
    found: set[str] = set()
    for table, column in sources.items():
        directory = output_dir / "_chunks" / table
        if not directory.is_dir():
            continue
        for path in directory.glob("*.parquet"):
            try:
                data = pq.read_table(path, columns=[column])
            except (KeyError, OSError):
                continue
            found.update(v for v in data.column(column).to_pylist() if v)
    return found


def probe_contrato_orgaos(
    session: requests.Session,
    candidates: list[str],
    *,
    year: int = CONTRATO_PROBE_YEAR,
    max_workers: int = 8,
) -> set[str]:
    """Return the candidates that hold at least one contract in `year`.

    Only about 6% of the 11,872 registered orgaos hold any contract, so probing
    once and expanding only the hits turns a 190k-request loop into roughly
    12k-24k. The probe is one request per orgao and the results are cached by
    the caller, since contrato and contrato_item share the same set.
    """
    spec = TABLE_SPECS["contrato"]
    assert spec.date_params
    lo_param, hi_param = spec.date_params

    def probe(orgao: str) -> tuple[str, int]:
        envelope = fetch_page(
            session,
            spec.path,
            {
                "codigoOrgao": orgao,
                lo_param: f"{year}-01-01",
                hi_param: f"{year}-12-31",
                "pagina": 1,
                "tamanhoPagina": 10,
            },
        )
        return orgao, int(envelope.get("totalRegistros") or 0)

    hits: set[str] = set()
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(probe, orgao) for orgao in candidates]
        for done, future in enumerate(as_completed(futures), start=1):
            try:
                orgao, count = future.result()
            except Exception:
                logger.exception("contrato probe failed")
                continue
            if count:
                hits.add(orgao)
            if done % 500 == 0:
                logger.info(
                    "contrato probe %d/%d, %d hits",
                    done,
                    len(candidates),
                    len(hits),
                )
    return hits


def list_registered_orgaos(session: requests.Session) -> list[str]:
    """Every orgao code in the registry, active and inactive."""
    codes: list[str] = []
    for status in ("true", "false"):
        page = 1
        while True:
            envelope = fetch_page(
                session,
                "/modulo-uasg/2_consultarOrgao",
                {
                    "statusOrgao": status,
                    "pagina": page,
                    "tamanhoPagina": PAGE_SIZE,
                },
            )
            rows = envelope.get("resultado") or []
            codes.extend(
                str(r["codigoOrgao"])
                for r in rows
                if r.get("codigoOrgao") is not None
            )
            if not rows or envelope.get("paginasRestantes", 0) <= 0:
                break
            page += 1
    return sorted(set(codes))
