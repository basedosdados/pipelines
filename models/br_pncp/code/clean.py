"""One-shot cleaning for the br_pncp onboarding.

Thin wrapper over ``utils.clean_table`` so the transform stays shared with the
recurring Prefect pipeline rather than duplicated.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from utils import DEDUP_KEYS, clean_table

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

    summaries = []
    for table in args.tables:
        if not (args.input_dir / table).exists():
            print(
                f"!! {table}: no raw chunks at {args.input_dir / table}, skipping",
                flush=True,
            )
            continue
        summary = clean_table(args.input_dir, args.output_dir, table)
        summaries.append(summary)
        dropped = (
            summary["raw_rows"]
            - summary["deduped_rows"]
            - summary["undated_dropped"]
        )
        print(
            f"{table}: raw={summary['raw_rows']:,} -> rows={summary['deduped_rows']:,} "
            f"(duplicates collapsed={dropped:,}, undated dropped={summary['undated_dropped']:,}) "
            f"years={summary['years'][0] if summary['years'] else '-'}"
            f"..{summary['years'][-1] if summary['years'] else '-'}",
            flush=True,
        )

    report = args.output_dir / "clean_summary.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(summaries, indent=2), encoding="utf-8")
    print(f"\nsummary written to {report}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
