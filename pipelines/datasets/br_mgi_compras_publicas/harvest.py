"""Job planning and execution for the br_mgi_compras_publicas harvest.

A *job* is the smallest independently resumable unit of work: one API query (or
one block of pages of it) whose cleaned output is written atomically to a single
parquet chunk. A run that dies halfway re-plans the same jobs and skips every
chunk already on disk, so restarting is free.
"""

from __future__ import annotations

import datetime as dt
import logging
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow.dataset as ds
import pyarrow.parquet as pq
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
    year_orgaos: dict[int, list[str]] | None = None,
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
        planner = session or build_session()
        for year in range(first_year, last_year + 1):
            if since and year < since.year:
                continue
            params = {**spec.params, spec.year_param: year}
            # A year is the smallest window this endpoint accepts, and a busy
            # one runs to 2,540 pages -- hours of work that would checkpoint
            # only at the very end. Splitting it by page range makes an
            # interrupted run cost minutes instead of the whole table.
            for job in _page_range_jobs(planner, spec, f"y{year}", params):
                jobs.append(
                    Job(
                        job.table,
                        job.chunk_id,
                        job.params,
                        year_fallback=year,
                        page_from=job.page_from,
                        page_to=job.page_to,
                    )
                )

    elif spec.window is WindowKind.YEAR_ORGAO:
        assert spec.year_param and spec.orgao_param
        if year_orgaos is None:
            raise ValueError(
                f"{spec.table} is partitioned by (year, orgao); pass year_orgaos "
                "derived from the harvested parent table"
            )
        planner = session or build_session()
        for year in range(first_year, last_year + 1):
            if since and year < since.year:
                continue
            for orgao in year_orgaos.get(year, []):
                params = {
                    **spec.params,
                    spec.year_param: year,
                    spec.orgao_param: orgao,
                }
                for job in _page_range_jobs(
                    planner, spec, f"y{year}__o{orgao}", params
                ):
                    jobs.append(
                        Job(
                            job.table,
                            job.chunk_id,
                            job.params,
                            year_fallback=year,
                            page_from=job.page_from,
                            page_to=job.page_to,
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
    year_orgaos: dict[int, list[str]] | None = None,
    since: dt.date | None = None,
    today: dt.date | None = None,
    extraction_date: dt.date | None = None,
    progress_every: int | None = None,
) -> dict[str, int]:
    """Harvest one table into resumable parquet chunks."""
    spec = TABLE_SPECS[table]
    columns = load_architecture(table)
    planner = build_session()
    planned = plan_jobs(
        spec,
        today=today,
        orgaos=orgaos,
        year_orgaos=year_orgaos,
        since=since,
        session=planner,
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

    # Report often enough that a long table shows movement, without spamming
    # a table with thousands of jobs.
    every = progress_every or max(1, min(50, len(todo) // 20 or 1))
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
            if done % every == 0:
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

#: The densest recent 365 days of contract starts, straddling two calendar
#: years so the single probe is not pinned to one. It stops before the feed
#: stalled on 2026-07-23, so the window is complete.
CONTRATO_PROBE_WINDOW = ("2024-07-01", "2025-06-30")


def orgaos_from_chunks(output_dir: Path) -> set[str]:
    """Orgao codes already visible in harvested chunks.

    Used to widen the probe's candidate list, not to shortcut it: 3,094 orgao
    codes appear in harvested contratacoes without existing in the orgao
    registry at all, and most orgaos hold no contract, so these still have to be
    probed. Expanding an unprobed orgao across every year would cost 17 requests
    to learn nothing.
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
    window: tuple[str, str] = CONTRATO_PROBE_WINDOW,
    max_workers: int = 8,
) -> set[str]:
    """Return the candidates holding at least one contract in `window`.

    Only 3-6% of orgaos hold any contract, so probing once and expanding only
    the hits turns a 250k-request loop into roughly 30k. One request per orgao;
    the caller caches the result because contrato and contrato_item share it.

    A single recent window could in principle miss an orgao whose contracts are
    all historical. Measured against that: of 390 orgaos sampled across two
    draws, 12 hold contracts, and every one of them also holds recent ones --
    including all 7 found by probing 2015 alone. No history-only orgao was
    observed, so the residual risk is small but not zero, and it is why the
    window straddles two calendar years rather than pinning to one.
    """
    spec = TABLE_SPECS["contrato"]
    assert spec.date_params
    lo_param, hi_param = spec.date_params
    lo, hi = window

    def probe(orgao: str) -> tuple[str, int]:
        envelope = fetch_page(
            session,
            spec.path,
            {
                "codigoOrgao": orgao,
                lo_param: lo,
                hi_param: hi,
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


def consolidate_table(
    table: str,
    output_dir: Path,
    *,
    jobs: list[Job] | None = None,
    prune: bool = False,
) -> dict[str, int]:
    """Merge a table's chunks into hive-partitioned parquet.

    Consolidates the chunks of the *planned job set* rather than whatever parquet
    happens to sit in the directory. Globbing would silently fold in chunks left
    by an earlier run whose job identifiers differed, double counting rows;
    reading the plan instead also surfaces chunks that are missing rather than
    quietly shipping a short table.

    Streams through pyarrow's dataset writer -- licitacao_item alone is tens of
    millions of rows.
    """
    chunk_dir = output_dir / "_chunks" / table
    if jobs is not None:
        files = [job.chunk_path(output_dir) for job in jobs]
        missing = [f for f in files if not f.exists()]
        if missing:
            logger.error(
                "%s: %d of %d planned chunks are missing; refusing to "
                "consolidate a partial table (first missing: %s)",
                table,
                len(missing),
                len(files),
                missing[0].name,
            )
            return {"rows": 0, "files": 0, "missing": len(missing)}
    else:
        if not chunk_dir.is_dir():
            return {"rows": 0, "files": 0, "missing": 0}
        files = sorted(chunk_dir.glob("*.parquet"))
    if not files:
        return {"rows": 0, "files": 0, "missing": 0}

    columns = [c.name for c in load_architecture(table)]
    partition_key = "ano" if "ano" in columns else "data_extracao"
    target = output_dir / "output" / table
    if target.exists():
        shutil.rmtree(target)

    dataset = ds.dataset(files, format="parquet")
    rows = sum(pq.read_metadata(f).num_rows for f in files)
    ds.write_dataset(
        dataset,
        target,
        format="parquet",
        partitioning=ds.partitioning(
            dataset.schema.empty_table().select([partition_key]).schema,
            flavor="hive",
        ),
        existing_data_behavior="overwrite_or_ignore",
        file_options=ds.ParquetFileFormat().make_write_options(
            compression="snappy"
        ),
        max_rows_per_file=2_000_000,
        max_rows_per_group=200_000,
        basename_template="data-{i}.parquet",
    )
    logger.info(
        "%s: consolidated %d rows from %d chunks -> %s",
        table,
        rows,
        len(files),
        target,
    )
    if prune:
        # Halves peak disk. Only safe once the consolidated output exists, and
        # it does cost a full re-harvest of this table if it is later needed.
        shutil.rmtree(chunk_dir)
        logger.info("%s: pruned chunk directory", table)
    return {"rows": rows, "files": len(files), "missing": 0}


def year_orgao_pairs(
    output_dir: Path, parent_table: str = "compra_sem_licitacao"
) -> dict[int, list[str]]:
    """(year -> orgao codes) taken from an already-harvested parent table.

    A child endpoint that only accepts `dt_ano_aviso_licitacao` paginates too
    deep to harvest a whole year at once. Its parent carries the orgao of every
    compra, so the parent's distinct (ano, codigo_orgao) pairs are exactly the
    partitions the child needs -- no guessing, and no probing for empty ones.
    """
    root = output_dir / "output" / parent_table
    if not root.is_dir():
        raise FileNotFoundError(
            f"{parent_table} must be consolidated before {root.name} can be "
            "partitioned by (year, orgao); run --consolidate on it first"
        )
    table = ds.dataset(root, format="parquet", partitioning="hive").to_table(
        columns=["ano", "codigo_orgao"]
    )
    pairs: dict[int, set[str]] = {}
    for year, orgao in zip(
        table.column("ano").to_pylist(),
        table.column("codigo_orgao").to_pylist(),
        strict=True,
    ):
        if year is None or not orgao:
            continue
        pairs.setdefault(int(year), set()).add(str(orgao))
    out = {y: sorted(v) for y, v in sorted(pairs.items())}
    logger.info(
        "%s: %d (year, orgao) partitions across %d years",
        parent_table,
        sum(len(v) for v in out.values()),
        len(out),
    )
    return out
