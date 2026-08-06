"""
Build partitioned parquet for the br_senado_dados_abertos onboarding.

  uv run python models/br_senado_dados_abertos/code/run_onboarding.py --sample
  uv run python models/br_senado_dados_abertos/code/run_onboarding.py --full

All-STRING parquet under code/output/<table>/. Reuses the pipeline's cleaning
transform (pipelines.datasets.br_senado_dados_abertos) — the single source of
truth for the extract, shared with the recurring pipeline. `upload.py` then
uploads code/output/ to BigQuery.
"""

from __future__ import annotations

import argparse
import glob
import os

import pyarrow.parquet as pq

from pipelines.datasets.br_senado_dados_abertos.utils import (
    ALL_TABLES,
    clean_all,
)

OUTPUT = os.path.join(os.path.dirname(__file__), "output")


def _rows(out_dir: str) -> int:
    files = glob.glob(os.path.join(out_dir, "**", "*.parquet"), recursive=True)
    return sum(pq.read_metadata(f).num_rows for f in files)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", action="store_true", help="recent slice, fast")
    ap.add_argument("--full", action="store_true", help="full history")
    args = ap.parse_args()
    # Sample: 2024 only for the time-series (dimensions are always full).
    years = None if args.full else range(2024, 2025)
    result = clean_all(OUTPUT, years=years)
    print("\n=== SUMMARY ===")
    for t in ALL_TABLES:
        print(f"  {t:32} {_rows(result[t]):>8} rows")
    print(f"  max_data_sessao: {result['max_data_sessao']}")


if __name__ == "__main__":
    main()
