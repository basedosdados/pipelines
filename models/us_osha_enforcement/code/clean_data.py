#!/usr/bin/env python
"""One-shot onboarding bootstrap for ``us_osha_enforcement``.

Downloads the OSHA bulk files and cleans them into partitioned, all-STRING
Parquet. The cleaning transform is *not* duplicated here: it is imported from
``pipelines/datasets/us_osha_enforcement/utils.py``, which the recurring
Prefect flow also uses, so the two can never drift.

    PYTHONPATH=$PWD python models/us_osha_enforcement/code/clean_data.py --all
    PYTHONPATH=$PWD python models/us_osha_enforcement/code/clean_data.py \
        --tables dicionario accident accident_narrative

Scratch data lives under ``~/Downloads/us_osha_enforcement_data`` — never in
the repo or in Dropbox, since the raw zips run to 6 GB.
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from pathlib import Path

from pipelines.datasets.us_osha_enforcement.constants import constants
from pipelines.datasets.us_osha_enforcement.utils import (
    clean_all,
    download_all,
    download_file,
)

log = logging.getLogger("us_osha_enforcement")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data-dir", default=constants.DEFAULT_DATA_DIR.value)
    p.add_argument("--all", action="store_true", help="clean every table")
    p.add_argument("--tables", nargs="*", default=None)
    p.add_argument(
        "--years",
        nargs="*",
        type=int,
        default=None,
        help="restrict output to these partition years",
    )
    p.add_argument("--download", action="store_true", help="download first")
    p.add_argument("--only-download", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    root = Path(args.data_dir).expanduser()
    input_dir, output_dir = root / "input", root / "output"

    if args.download or args.only_download:
        download_all(input_dir)
    if args.only_download:
        return 0
    if not args.all and not args.tables:
        p.error("pass --all or --tables")

    missing = [
        stem
        for stem in constants.SOURCE_FILES.value
        if not (input_dir / f"OSHA_{stem}.zip").exists()
    ]
    if missing:
        log.info(f"downloading {len(missing)} missing file(s): {missing}")
        for stem in missing:
            download_file(stem, input_dir)

    free = shutil.disk_usage(root).free
    if free < 6 << 30:
        log.warning(f"only {free / 1e9:.1f} GB free under {root}")

    # A table is rebuilt from scratch: a stale partition left from an earlier
    # layout would otherwise be shipped, because bd.Table.create uploads the
    # whole directory.
    for slug in args.tables or []:
        shutil.rmtree(output_dir / slug, ignore_errors=True)
    if args.all and not args.years:
        shutil.rmtree(output_dir, ignore_errors=True)

    counts = clean_all(
        input_dir,
        output_dir,
        args.tables,
        set(args.years) if args.years else None,
    )
    report = output_dir / "clean_report.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    existing = json.loads(report.read_text()) if report.exists() else {}
    existing.update(counts)
    report.write_text(json.dumps(existing, indent=1, sort_keys=True))

    total = sum(counts.values())
    for slug, rows in counts.items():
        log.info(f"  {slug:<22} {rows:>12,}")
    log.info(f"  {'TOTAL':<22} {total:>12,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
