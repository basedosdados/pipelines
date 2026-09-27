"""One-shot bootstrap: clean the Météo-France sources into partitioned parquet.

This is the CLI wrapper around the transform. The transform itself lives in
``pipelines/datasets/fr_meteofrance/utils.py`` and is shared verbatim with the
recurring Prefect pipeline — there is deliberately no second copy of it here.

    uv run python models/fr_meteofrance/code/clean.py [--only synop|normales]

Tables produced under ``$MF_OUTPUT`` (default ``~/Downloads/fr_meteofrance_data/output``):

* ``synop/annee=<year>/data.parquet``       3-hourly SYNOP observations, 1996-2026
* ``station_synop/data.parquet``            the SYNOP station register
* ``station_climatologique/data.parquet``   the climate-normals station register
* ``normale_climatologique/data.parquet``   1991-2020 normals and records, long format
* ``dicionario/data.parquet``               the committed WMO/BUFR code tables

Input is read from ``$MF_INPUT`` (default ``~/Downloads/fr_meteofrance_data/input``),
which must already hold the downloaded archives and sheets.
"""

import argparse
import os
import sys
from pathlib import Path

# The transform lives in the pipelines package; make the repo root importable
# when this script is run directly rather than as a module.
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.fr_meteofrance.utils import (
    clean_normales,
    clean_station_synop,
    clean_synop,
    write_dicionario,
)

INPUT = Path(
    os.path.expanduser(
        os.environ.get("MF_INPUT", "~/Downloads/fr_meteofrance_data/input")
    )
)
OUTPUT = Path(
    os.path.expanduser(
        os.environ.get("MF_OUTPUT", "~/Downloads/fr_meteofrance_data/output")
    )
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--only", choices=["synop", "normales"], help="clean a single group"
    )
    args = parser.parse_args()

    print(f"input  {INPUT}\noutput {OUTPUT}")
    if args.only != "normales":
        result = clean_synop(INPUT, OUTPUT)
        print(f"  synop: {result['rows']:,} rows, latest {result['max_date']}")
        path = clean_station_synop(result["stations"], INPUT, OUTPUT)
        print(f"  station_synop -> {path}")
    if args.only != "synop":
        result = clean_normales(INPUT / "ficheclim", OUTPUT)
        print(f"  station_climatologique: {result['stations']} rows")
        print(
            f"  normale_climatologique: {result['rows']:,} rows, "
            f"latest edition {result['max_date']}"
        )
        print(f"  dicionario -> {write_dicionario(OUTPUT)}")


if __name__ == "__main__":
    main()
