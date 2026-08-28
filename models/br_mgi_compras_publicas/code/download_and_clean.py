"""One-shot backfill driver for br_mgi_compras_publicas.

Imports the harvest and cleaning code from ``pipelines/datasets/`` rather than
duplicating it, so the recurring flow and this backfill can never drift.

Scratch data never goes in the repo or under Dropbox. It defaults to
``~/Downloads/br_mgi_compras_publicas_data`` and is deleted once the dataset is
published; everything here is reproducible from the API.

Usage
-----
    uv run python models/br_mgi_compras_publicas/code/download_and_clean.py \
        --tables fast
    uv run python models/br_mgi_compras_publicas/code/download_and_clean.py \
        --tables contrato contrato_item
    uv run python models/br_mgi_compras_publicas/code/download_and_clean.py \
        --consolidate

Every step is resumable: a chunk on disk is never re-fetched.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import shutil
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pyarrow.dataset as ds  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

from pipelines.datasets.br_mgi_compras_publicas.api import (  # noqa: E402
    build_session,
    limiter_rates,
)
from pipelines.datasets.br_mgi_compras_publicas.harvest import (  # noqa: E402
    harvest_table,
    list_registered_orgaos,
    orgaos_from_chunks,
    plan_jobs,
    probe_contrato_orgaos,
)
from pipelines.datasets.br_mgi_compras_publicas.utils import (  # noqa: E402
    TABLE_SPECS,
    load_architecture,
)

logger = logging.getLogger("br_mgi_compras_publicas")

DEFAULT_DATA_DIR = Path.home() / "Downloads" / "br_mgi_compras_publicas_data"

# Tables whose module is not savagely rate limited. These are the bulk of the
# rows and can run unattended; contrato and contrato_item are separate because
# /modulo-contratos/ converges to roughly 0.6 req/s.
FAST_TABLES = [
    "orgao",
    "unidade_administrativa",
    "catalogo_material",
    "catalogo_servico",
    "fornecedor",
    "contratacao",
    "ata_registro_preco",
    "ata_registro_preco_item",
    "licitacao",
    "licitacao_pregao",
    "compra_sem_licitacao",
    "compra_sem_licitacao_item",
    "contratacao_item",
    "contratacao_item_resultado",
    "licitacao_item_pregao",
    "licitacao_item",
]
SLOW_TABLES = ["contrato", "contrato_item"]
ALL_TABLES = FAST_TABLES + SLOW_TABLES


def data_dir() -> Path:
    return Path(os.environ.get("COMPRAS_DATA_DIR", DEFAULT_DATA_DIR))


def resolve_orgaos(output_dir: Path, *, probe: bool) -> list[str]:
    """Orgao codes to iterate for the contrato tables, cached on disk.

    Two sources, unioned. Orgaos already visible in harvested procurement, which
    costs nothing; and optionally a one-request-per-orgao probe of the registry,
    which costs about 11,900 requests once but catches carona contracts signed
    by an orgao that ran no procurement of its own.
    """
    cache = output_dir / "_meta" / "contrato_orgaos.json"
    if cache.exists():
        codes = json.loads(cache.read_text())["orgaos"]
        logger.info("orgao list: %d from cache", len(codes))
        return codes

    from_data = {str(code) for code in orgaos_from_chunks(output_dir)}
    logger.info("orgao list: %d seen in harvested procurement", len(from_data))

    hits: set[str] = set()
    if probe:
        session = build_session()
        registered = list_registered_orgaos(session)
        candidates = [code for code in registered if code not in from_data]
        logger.info(
            "probing %d registered orgaos not already seen (of %d registered)",
            len(candidates),
            len(registered),
        )
        hits = probe_contrato_orgaos(session, candidates)
        logger.info(
            "probe found %d additional orgaos holding contracts", len(hits)
        )

    codes = sorted(from_data | hits)
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(
        json.dumps(
            {
                "orgaos": codes,
                "from_harvested_data": len(from_data),
                "from_probe": len(hits),
                "probed": probe,
                "built_at": dt.datetime.now().isoformat(timespec="seconds"),
            },
            indent=1,
        )
    )
    logger.info("orgao list: %d total, cached at %s", len(codes), cache)
    return codes


def consolidate(
    table: str,
    output_dir: Path,
    *,
    jobs: list | None = None,
    prune: bool = False,
) -> dict[str, int]:
    """Merge a table's chunks into hive-partitioned output.

    Consolidates the chunks of the *planned job set* rather than whatever
    parquet happens to sit in the directory. Globbing would silently fold in
    chunks left by an earlier run whose job identifiers differed, double
    counting rows; reading the plan instead also surfaces chunks that are
    missing rather than quietly shipping a short table.

    Streams through pyarrow's dataset writer: contratacao_item alone is
    millions of rows.
    """
    chunk_dir = output_dir / "_chunks" / table
    if jobs is not None:
        files = [job.chunk_path(output_dir) for job in jobs]
        missing = [f for f in files if not f.exists()]
        if missing:
            logger.error(
                "%s: %d of %d planned chunks are missing; refusing to consolidate "
                "a partial table (first missing: %s)",
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tables",
        nargs="+",
        default=["fast"],
        help="table names, or one of: fast, slow, all",
    )
    parser.add_argument(
        "--consolidate", action="store_true", help="only consolidate chunks"
    )
    parser.add_argument("--workers", type=int, default=None)
    parser.add_argument(
        "--probe-orgaos",
        action="store_true",
        help="probe every registered orgao for contracts before the contrato loop",
    )
    parser.add_argument("--output-dir", type=Path, default=None)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    output_dir = args.output_dir or data_dir()
    output_dir.mkdir(parents=True, exist_ok=True)

    groups = {"fast": FAST_TABLES, "slow": SLOW_TABLES, "all": ALL_TABLES}
    tables: list[str] = []
    for name in args.tables:
        tables.extend(groups.get(name, [name]))
    unknown = [t for t in tables if t not in TABLE_SPECS]
    if unknown:
        parser.error(f"unknown tables: {unknown}")

    if args.consolidate:
        planner = build_session()
        failed = 0
        for table in tables:
            spec = TABLE_SPECS[table]
            orgaos = (
                resolve_orgaos(output_dir, probe=False)
                if spec.window == "orgao"
                else None
            )
            jobs = plan_jobs(spec, orgaos=orgaos, session=planner)
            stats = consolidate(
                table, output_dir, jobs=jobs, prune=args.prune_chunks
            )
            failed += stats.get("missing", 0) > 0
        return 1 if failed else 0

    extraction_date = dt.date.today()
    summary: dict[str, dict[str, int]] = {}
    for table in tables:
        spec = TABLE_SPECS[table]
        orgaos = None
        if spec.window.value == "orgao":
            orgaos = resolve_orgaos(output_dir, probe=args.probe_orgaos)
        started = time.time()
        summary[table] = harvest_table(
            table,
            output_dir,
            max_workers=args.workers,
            orgaos=orgaos,
            extraction_date=extraction_date,
        )
        summary[table]["seconds"] = int(time.time() - started)
        logger.info("rate limiters: %s", limiter_rates())

    print("\n=== harvest summary ===")
    for table, stats in summary.items():
        print(
            f"  {table:<28} rows={stats['rows']:>10,}  jobs={stats['ran']:>6}"
            f"  failed={stats['failures']:>4}  {stats['seconds']:>6}s"
        )
    return 1 if any(s["failures"] for s in summary.values()) else 0


if __name__ == "__main__":
    raise SystemExit(main())
