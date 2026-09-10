"""Build the `dicionario` table from the cleaned complaint parquet.

The builder lives in ``pipelines/datasets/us_cfpb_complaints/utils.py`` and is shared
with the recurring pipeline, which regenerates the register on every run; this is only
the CLI around it.

The CFPB publishes its categorical fields as readable English labels rather than codes,
so `valor` reproduces `chave`. What the register adds is `cobertura_temporal`: the years
in which each value actually appears, which exposes the April 2017 and August 2023
revisions of the complaint form's taxonomy. Values are preserved as published, never
remapped, so the same concept shows up under several labels with disjoint coverage.
"""

import argparse
from pathlib import Path

from common import OUTPUT, build_dicionario

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--output", type=Path, default=OUTPUT)
    build_dicionario(ap.parse_args().output)
