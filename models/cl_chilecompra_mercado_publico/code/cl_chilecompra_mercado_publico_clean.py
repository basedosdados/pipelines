"""One-shot historical load for cl_chilecompra_mercado_publico.

Downloads every monthly ZIP, cleans it, writes hive-partitioned parquet, and deletes
the raw archive before moving on -- the full history is ~270 GB uncompressed, so only
one month is ever on disk at a time (~600 MB peak).

The transform itself lives in ``pipelines.datasets.cl_chilecompra_mercado_publico.utils``
and is shared verbatim with the recurring Prefect pipeline.

    uv run python models/cl_chilecompra_mercado_publico/code/cl_chilecompra_mercado_publico_clean.py \
        --start 2007-1 --end 2026-8

Resumable: a month whose parquet parts already exist is skipped unless --force.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path

from pipelines.datasets.cl_chilecompra_mercado_publico import utils

DEFAULT_ROOT = Path(
    os.environ.get(
        "CHILECOMPRA_DATA_DIR",
        Path.home() / "Downloads" / "cl_chilecompra_mercado_publico_data",
    )
)


def parse_ym(text: str) -> tuple[int, int]:
    year, month = text.split("-")
    return int(year), int(month)


def months(start: tuple[int, int], end: tuple[int, int]):
    y, m = start
    while (y, m) <= end:
        yield y, m
        m += 1
        if m > 12:
            y, m = y + 1, 1


def month_done(output_dir: Path, kind: str, year: int, month: int) -> bool:
    return all(
        (
            output_dir
            / t
            / f"ano={year}"
            / f"mes={month:02d}"
            / "data.parquet"
        ).exists()
        for t in utils.TABLES_BY_KIND[kind]
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2007-1")
    ap.add_argument("--end", default="2026-8")
    ap.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    ap.add_argument("--kinds", default="orden_compra,licitacion")
    ap.add_argument("--force", action="store_true")
    ap.add_argument(
        "--keep-raw",
        action="store_true",
        help="do not delete the ZIP after cleaning",
    )
    args = ap.parse_args()

    input_dir = args.root / "input"
    output_dir = args.root / "output"
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = args.root / "clean_log.jsonl"

    kinds = [k.strip() for k in args.kinds.split(",") if k.strip()]
    todo = [
        (k, y, m)
        for y, m in months(parse_ym(args.start), parse_ym(args.end))
        for k in kinds
    ]
    print(f"{len(todo)} month-files to process -> {output_dir}", flush=True)

    failures = []
    for i, (kind, year, month) in enumerate(todo, 1):
        tag = f"[{i}/{len(todo)}] {kind} {year}-{month:02d}"
        if not args.force and month_done(output_dir, kind, year, month):
            print(f"{tag} skip (already built)", flush=True)
            continue
        started = datetime.now()
        try:
            if utils.head_month(kind, year, month) is None:
                print(f"{tag} absent at source (404), skipping", flush=True)
                continue
            zip_path = utils.download_month(kind, year, month, input_dir)
            frames = utils.clean_month(kind, zip_path)
            counts = {}
            for table, df in frames.items():
                utils.write_partitioned(df, table, output_dir)
                counts[table] = len(df)
            if not args.keep_raw:
                zip_path.unlink(missing_ok=True)
            secs = (datetime.now() - started).total_seconds()
            print(f"{tag} ok {counts} in {secs:.0f}s", flush=True)
            with open(log_path, "a", encoding="utf-8") as fh:
                fh.write(
                    json.dumps(
                        {
                            "kind": kind,
                            "year": year,
                            "month": month,
                            "rows": counts,
                            "seconds": round(secs, 1),
                        }
                    )
                    + "\n"
                )
        except Exception as exc:
            failures.append((kind, year, month, repr(exc)))
            print(f"{tag} FAILED: {exc}", flush=True)
            traceback.print_exc()

    if failures:
        print(f"\n{len(failures)} failures:", flush=True)
        for f in failures:
            print("  ", f, flush=True)
        return 1
    print("\nall months processed", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
