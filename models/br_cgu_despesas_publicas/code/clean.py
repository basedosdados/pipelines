"""Clean the downloaded br_cgu_despesas_publicas months into partitioned parquet.

Usage:
    uv run python models/br_cgu_despesas_publicas/code/clean.py

Reads the raw monthly CSVs from ``$BR_CGU_DESPESAS_DATA/input`` (default
``~/Downloads/br_cgu_despesas_publicas_data``) and writes all-STRING Snappy
parquet to ``$BR_CGU_DESPESAS_DATA/output/execucao/ano=<y>/mes=<m>/data.parquet``.

The transform itself lives in ``pipelines/datasets/br_cgu_despesas_publicas/utils.py``
so the one-shot onboarding and the recurring flow share exactly one copy of it.
Scratch data deliberately lives outside the repo and outside Dropbox.
"""

import json
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.br_cgu_despesas_publicas.utils import (
    clean_all,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

DATA = Path(
    os.environ.get(
        "BR_CGU_DESPESAS_DATA",
        Path.home() / "Downloads" / "br_cgu_despesas_publicas_data",
    )
)


def main() -> None:
    input_dir = DATA / "input"
    months = sorted(
        (int(p.stem[:4]), int(p.stem[4:])) for p in input_dir.glob("*.csv")
    )
    if not months:
        raise SystemExit(f"No monthly CSVs found under {input_dir}")
    print(f"cleaning {len(months)} months: {months[0]} .. {months[-1]}")
    result = clean_all(input_dir, DATA / "output", months)
    summary = DATA / "clean_summary.json"
    summary.write_text(
        json.dumps(
            {
                "months": len(result["rows"]),
                "total_rows": result["total"],
                "max_period": result["max_period"],
                "rows": result["rows"],
            },
            indent=1,
        )
    )
    print(f"\ntotal rows: {result['total']:,}")
    print(f"max period: {result['max_period']}")
    print(f"summary   : {summary}")


if __name__ == "__main__":
    main()
