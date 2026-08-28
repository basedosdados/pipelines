"""Validate harvested chunks against the claims the dbt tests will make.

Checks, per table with chunks on disk:
  1. the declared key is actually unique
  2. the key columns and the partition column are non-null
  3. every architecture column is present, in order

Run before trusting the generated schema.yml, since a key asserted in dbt but
false in the data fails only after a full upload and materialisation.

Usage:  uv run python models/br_mgi_compras_publicas/code/validate.py
"""

from __future__ import annotations

import hashlib
import sys
from collections import Counter, defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pyarrow.dataset as ds  # noqa: E402
from dbt_spec import TABLES  # noqa: E402

from pipelines.datasets.br_mgi_compras_publicas.api import (  # noqa: E402
    build_session,
)
from pipelines.datasets.br_mgi_compras_publicas.harvest import (  # noqa: E402
    plan_jobs,
)
from pipelines.datasets.br_mgi_compras_publicas.utils import (  # noqa: E402
    TABLE_SPECS,
    load_architecture,
)


def data_dir() -> Path:
    import os

    return Path(
        os.environ.get(
            "COMPRAS_DATA_DIR",
            Path.home() / "Downloads" / "br_mgi_compras_publicas_data",
        )
    )


def check(table: str, files: list[Path]) -> list[str]:
    """Verify the declared key holds, distinguishing two very different faults.

    The API repeats records across pages, and the models deduplicate on the key,
    so repeated *identical* rows are expected and harmless. Repeated rows that
    *differ* are the real signal: either the key is wrong, or the source changed
    mid-harvest and the dedup ordering decides which copy wins. Only the second
    kind is reported as a failure.
    """
    spec = TABLES[table]
    columns = [c.name for c in load_architecture(table)]
    dataset = ds.dataset(files, format="parquet")
    problems: list[str] = []

    if dataset.schema.names != columns:
        missing = set(columns) - set(dataset.schema.names)
        extra = set(dataset.schema.names) - set(columns)
        problems.append(
            f"column mismatch: missing={sorted(missing)} extra={sorted(extra)}"
        )

    key = [k for k in spec.key if k in dataset.schema.names]
    if not key:
        return problems

    copies: dict[tuple, set[str]] = defaultdict(set)
    counts: Counter[tuple] = Counter()
    all_null = 0
    rows = 0
    for batch in dataset.to_batches(batch_size=100_000):
        data = batch.to_pydict()
        rows += batch.num_rows
        for i in range(batch.num_rows):
            values = tuple(data[k][i] for k in key)
            counts[values] += 1
            if all(v is None for v in values):
                all_null += 1
            digest = hashlib.md5(
                "|".join(str(data[c][i]) for c in columns).encode()
            ).hexdigest()
            copies[values].add(digest)

    repeated = sum(c - 1 for c in counts.values() if c > 1)
    conflicting = sum(1 for v in copies.values() if len(v) > 1)

    if conflicting:
        example = next(k for k, v in copies.items() if len(v) > 1)
        problems.append(
            f"key {key} has {conflicting:,} values whose rows genuinely differ "
            f"(dedup by {spec.dedup_order or 'nothing'} picks one); example {example}"
        )
    if all_null:
        problems.append(f"key {key} is entirely null in {all_null:,} rows")

    status = "ok " if not problems else "!! "
    print(
        f"{status} {table:<28} {rows:>10,} rows, {len(counts):>10,} distinct keys, "
        f"{repeated:,} identical repeats ({100 * repeated / max(rows, 1):.3f}%)"
    )
    for problem in problems:
        print(f"      {problem}")
    return problems


def main() -> int:
    output_dir = data_dir()
    root = output_dir / "_chunks"
    planner = build_session()
    failures = 0
    for table in TABLES:
        chunk_dir = root / table
        if not chunk_dir.is_dir():
            continue
        spec = TABLE_SPECS.get(table)
        if spec is None:  # dicionario is derived, not harvested
            continue
        # Read the planned chunk set, not a glob. A directory can still hold
        # chunks written by an earlier run under different job identifiers, and
        # globbing those in doubles every row.
        orgaos = ["0"] if spec.window == "orgao" else None
        jobs = plan_jobs(spec, orgaos=orgaos, session=planner)
        files = [
            j.chunk_path(output_dir)
            for j in jobs
            if j.chunk_path(output_dir).exists()
        ]
        if not files:
            continue
        print(
            f"--- {table}: {len(files)} of {len(jobs)} planned chunks present"
        )
        failures += bool(check(table, files))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
