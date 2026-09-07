"""Audit br_pncp harvest coverage without touching the API.

Two failure modes matter, and neither is caught by dbt:

1. **A window was never fetched.** Its rows simply do not exist, so every
   test still passes -- row counts look plausible, keys are unique, the
   dictionary covers what is there. For `contratacao` this is sharpest per
   MODALIDADE: losing modalidade 8 would drop most Dispensa de Licitação
   while the table still looks healthy.

2. **A window was fetched but truncated.** Every page line logs
   `page N/total`, so a complete window ends at `page total/total`. If the
   highest page reached is below the total, paging stopped early.

   Do NOT infer this from record counts. PNCP's `totalPaginas` is not
   computed against the `tamanhoPagina` we ask for, and per-page yield
   varies wildly -- one real 3-page window returned 50, 50 and 243 records.
   A floor of `(pages - 1) * page_size` flags ten complete windows as
   truncated, which is how this check was first written.

Both are answered from the chunk files and the harvest logs, so this costs
the API nothing and can be run while a harvest is in flight.

Usage:
    uv run python models/br_pncp/code/audit_coverage.py [--table ...]
"""

from __future__ import annotations

import argparse
import gzip
import os
import re
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.br_pncp.constants import constants
from pipelines.datasets.br_pncp.utils import windows

DATA_DIR = Path(
    os.environ.get("PNCP_DATA_DIR", Path.home() / "Downloads" / "br_pncp_data")
)
PAGE_LINE = re.compile(r"(\S+) (\d{8}_\d{8}(?:_m\d{2})?) page (\d+)/(\d+)")


def expected_tags(table: str, start: date, end: date) -> list[str]:
    spec: dict[str, Any] = constants.ENDPOINTS.value[table]
    rz = spec.get("resize")
    rz = (date.fromisoformat(rz[0]), int(rz[1])) if rz else None
    mods = constants.MODALIDADES.value if spec["by_modalidade"] else [None]
    out = []
    for lo, hi in windows(start, end, int(spec["window_days"]), rz):
        for m in mods:
            tag = f"{lo:%Y%m%d}_{hi:%Y%m%d}"
            if m is not None:
                tag += f"_m{m:02d}"
            out.append(tag)
    return out


def paging_progress(log_dir: Path) -> dict[tuple[str, str], tuple[int, int]]:
    """(highest page reached, totalPaginas) per (table, window).

    A window whose chunk exists but never reached its last page was written
    short. Keyed on the best attempt seen across all logs, since a window
    may have been retried across runs.
    """
    seen: dict[tuple[str, str], tuple[int, int]] = {}
    for log in sorted(log_dir.glob("harvest*.log")):
        with log.open(encoding="utf-8", errors="replace") as fh:
            for line in fh:
                m = PAGE_LINE.search(line)
                if not m:
                    continue
                key = (m.group(1), m.group(2))
                page, total = int(m.group(3)), int(m.group(4))
                best = seen.get(key, (0, 0))
                if page > best[0]:
                    seen[key] = (page, total)
    return seen


def audit(table: str, start: date, end: date, log_dir: Path) -> int:
    spec = constants.ENDPOINTS.value[table]
    d = DATA_DIR / "input" / table
    want = expected_tags(table, start, end)
    have = {p.name.replace(".jsonl.gz", "") for p in d.glob("*.jsonl.gz")}
    missing = [t for t in want if t not in have]

    print(f"\n=== {table}")
    print(f"  windows expected : {len(want)}")
    print(f"  windows on disk  : {len(have)}")
    print(f"  MISSING          : {len(missing)}")

    if spec["by_modalidade"]:
        by_mod: dict[str, int] = defaultdict(int)
        for t in missing:
            by_mod[t.rsplit("_m", 1)[1]] += 1
        if by_mod:
            print("  missing per modalidade (a spike here is the danger):")
            for m in sorted(by_mod):
                print(f"      m{m}: {by_mod[m]}")

    # truncation: paging never reached the window's last page
    progress = paging_progress(log_dir)
    short = []
    for p in sorted(d.glob("*.jsonl.gz")):
        tag = p.name.replace(".jsonl.gz", "")
        reached, total = progress.get((table, tag), (0, 0))
        if total and reached < total:
            with gzip.open(p, "rt", encoding="utf-8") as fh:
                n = sum(1 for _ in fh)
            short.append((tag, n, reached, total))
    print(f"  TRUNCATED chunks : {len(short)}")
    for tag, n, reached, total in short[:10]:
        print(f"      {tag}: stopped at page {reached}/{total} ({n:,} rows)")
    return len(missing) + len(short)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tables", nargs="*", default=constants.FACT_TABLES.value)
    ap.add_argument(
        "--start",
        type=date.fromisoformat,
        default=date(constants.START_YEAR.value, 1, 1),
    )
    ap.add_argument(
        "--end", type=date.fromisoformat, default=date(2026, 8, 28)
    )
    ap.add_argument(
        "--log-dir",
        type=Path,
        required=True,
        help="directory holding harvest*.log",
    )
    args = ap.parse_args()
    problems = 0
    for t in args.tables:
        problems += audit(t, args.start, args.end, args.log_dir)
    print(f"\n=== total problems: {problems}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
