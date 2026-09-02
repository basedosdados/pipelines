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
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipelines.datasets.br_mgi_compras_publicas.api import (  # noqa: E402
    build_session,
    limiter_rates,
)
from pipelines.datasets.br_mgi_compras_publicas.harvest import (  # noqa: E402
    CONTRATO_PROBE_WINDOW,
    harvest_table,
    list_registered_orgaos,
    orgaos_from_chunks,
    plan_jobs,
    probe_contrato_orgaos,
    year_orgao_pairs,
)
from pipelines.datasets.br_mgi_compras_publicas.harvest import (  # noqa: E402
    consolidate_table as consolidate,
)
from pipelines.datasets.br_mgi_compras_publicas.utils import (  # noqa: E402
    TABLE_SPECS,
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

    The candidate list is the orgao registry widened by every orgao code seen in
    harvested procurement -- 3,094 of those do not exist in the registry at all.
    Every candidate is then probed once, and only the ones holding a contract are
    expanded across the years.

    Probing the data-derived orgaos matters as much as probing the registry.
    Appearing in a contratacao says nothing about holding a contract, and most
    orgaos hold none: expanding an unprobed orgao costs 17 requests to learn
    nothing, which on 4,800 of them is roughly 38 hours at this module's 0.6
    req/s.
    """
    cache = output_dir / "_meta" / "contrato_orgaos.json"
    if cache.exists():
        codes = json.loads(cache.read_text())["orgaos"]
        logger.info("orgao list: %d from cache", len(codes))
        return codes

    from_data = {str(code) for code in orgaos_from_chunks(output_dir)}
    session = build_session()
    registered = set(list_registered_orgaos(session))
    candidates = sorted(registered | from_data)
    logger.info(
        "orgao candidates: %d registered + %d seen in harvested procurement "
        "(%d of those unregistered) = %d to probe",
        len(registered),
        len(from_data),
        len(from_data - registered),
        len(candidates),
    )

    if not probe:
        raise SystemExit(
            "contrato needs --probe-orgaos: expanding every candidate across all "
            f"years is {len(candidates) * 17:,} requests at ~0.6 req/s"
        )

    hits = probe_contrato_orgaos(session, candidates)
    logger.info(
        "probe: %d of %d orgaos hold a contract (%.1f%%)",
        len(hits),
        len(candidates),
        100 * len(hits) / max(len(candidates), 1),
    )

    codes = sorted(hits)
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(
        json.dumps(
            {
                "orgaos": codes,
                "candidates": len(candidates),
                "registered": len(registered),
                "from_harvested_data": len(from_data),
                "probe_window": list(CONTRATO_PROBE_WINDOW),
                "built_at": dt.datetime.now().isoformat(timespec="seconds"),
            },
            indent=1,
        )
    )
    logger.info(
        "orgao list: %d with contracts, cached at %s", len(codes), cache
    )
    return codes


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
    parser.add_argument(
        "--allow-missing",
        action="store_true",
        help="consolidate even if planned chunks are absent; the missing chunks "
        "are named in the log. Only for a gap that is documented in the table's "
        "description",
    )
    parser.add_argument(
        "--prune-chunks",
        action="store_true",
        help="delete each table's chunks once consolidated; halves peak disk but "
        "makes a later re-consolidation require re-harvesting",
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
                if spec.window.value == "orgao"
                else None
            )
            year_orgaos = (
                year_orgao_pairs(output_dir)
                if spec.window.value == "year_orgao"
                else None
            )
            jobs = plan_jobs(
                spec,
                orgaos=orgaos,
                year_orgaos=year_orgaos,
                session=planner,
            )
            stats = consolidate(
                table,
                output_dir,
                jobs=jobs,
                prune=args.prune_chunks,
                allow_missing=args.allow_missing,
            )
            failed += stats.get("missing", 0) > 0
        return 1 if failed else 0

    extraction_date = dt.date.today()
    summary: dict[str, dict[str, int]] = {}
    for table in tables:
        spec = TABLE_SPECS[table]
        orgaos = None
        year_orgaos = None
        if spec.window.value == "orgao":
            orgaos = resolve_orgaos(output_dir, probe=args.probe_orgaos)
        elif spec.window.value == "year_orgao":
            year_orgaos = year_orgao_pairs(output_dir)
        started = time.time()
        summary[table] = harvest_table(
            table,
            output_dir,
            max_workers=args.workers,
            orgaos=orgaos,
            year_orgaos=year_orgaos,
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
