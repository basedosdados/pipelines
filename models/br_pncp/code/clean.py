"""One-shot cleaning for the br_pncp onboarding.

Front end for ``pipelines.datasets.br_pncp.utils.clean_table``, which is the
canonical transform shared with the recurring pipeline. ``replace=True`` here:
the backfill produces the complete table, so the output tree is rebuilt from
scratch rather than merged into.

Usage:
    uv run python models/br_pncp/code/clean.py [--tables ...]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.br_pncp.utils import DEDUP_KEYS, clean_table

DATA_DIR = Path(
    os.environ.get("PNCP_DATA_DIR", Path.home() / "Downloads" / "br_pncp_data")
)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--tables",
        nargs="*",
        default=list(DEDUP_KEYS),
        choices=list(DEDUP_KEYS),
    )
    ap.add_argument("--input-dir", type=Path, default=DATA_DIR / "input")
    ap.add_argument("--output-dir", type=Path, default=DATA_DIR / "output")
    args = ap.parse_args()

    report = args.output_dir / "clean_summary.json"
    existing = (
        {s["table"]: s for s in json.loads(report.read_text(encoding="utf-8"))}
        if report.exists()
        else {}
    )

    for table in args.tables:
        if not (args.input_dir / table).exists():
            print(
                f"!! {table}: no raw chunks at {args.input_dir / table}, skipping"
            )
            continue
        summary = clean_table(
            args.input_dir, args.output_dir, table, replace=True
        )
        existing[table] = summary
        collapsed = (
            summary["raw_rows"]
            - summary["deduped_rows"]
            - summary["undated_dropped"]
        )
        years = summary["years"]
        print(
            f"{table}: raw={summary['raw_rows']:,} -> rows={summary['deduped_rows']:,} "
            f"(duplicates collapsed={collapsed:,}, "
            f"undated dropped={summary['undated_dropped']:,}) "
            f"years={years[0] if years else '-'}..{years[-1] if years else '-'}",
            flush=True,
        )

    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(
        json.dumps(list(existing.values()), indent=2), encoding="utf-8"
    )
    print(f"\nsummary written to {report}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
