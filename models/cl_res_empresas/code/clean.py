"""One-shot onboarding bootstrap for cl_res_empresas.

Downloads every yearly CSV published on datos.gob.cl and writes the partitioned
parquet output. The cleaning transform itself lives in
``pipelines/datasets/cl_res_empresas/utils.py`` and is shared with the recurring
Prefect pipeline.

    python models/cl_res_empresas/code/clean.py --download
    python models/cl_res_empresas/code/clean.py
"""

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.cl_res_empresas.utils import (
    clean_all,
    download_all,
)

DATA_DIR = Path(
    os.environ.get(
        "CL_RES_EMPRESAS_DATA",
        Path.home() / "Downloads" / "cl_res_empresas_data",
    )
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--download", action="store_true", help="fetch the raw CSVs first"
    )
    parser.add_argument(
        "--data-dir", default=str(DATA_DIR), help="scratch directory"
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    input_dir, output_dir = data_dir / "input", data_dir / "output"

    if args.download:
        paths = download_all(input_dir)
        print(f"downloaded {len(paths)} file(s) to {input_dir}")

    counts = clean_all(input_dir, output_dir)
    for key in sorted(counts):
        print(f"{key:>18}: {counts[key]:>9,}")
    print(f"\noutput written to {output_dir}")


if __name__ == "__main__":
    main()
