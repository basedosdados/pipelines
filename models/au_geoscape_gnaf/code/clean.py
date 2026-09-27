"""
One-shot bootstrap: clean Geoscape G-NAF (au_geoscape_gnaf) into partitioned
parquet.

Thin CLI wrapper over the shared transform in
``pipelines/datasets/au_geoscape_gnaf/utils.py`` (single source of truth, reused
by the recurring pipeline). This bootstrap writes **typed** parquet
(``stringify=False``) — the one-shot ``upload.py`` uploads it with an explicit
typed hive schema. The recurring pipeline instead writes all-STRING parquet, so
its ``upload_to_gcs`` staging schema (inferred from a stringified header) matches.

Run from the repo root so ``pipelines`` is importable, e.g.:
    uv run python models/au_geoscape_gnaf/code/clean.py

Env:
  GNAF_DATA_DIR   default ~/Downloads/au_geoscape_gnaf_data
  GNAF_ZIP        default <DATA_DIR>/input/g-naf_may26_gda2020.zip
  GNAF_SNAPSHOT   default 2026-05-01   (first day of the release month)
  GNAF_STATES     default all          (comma list, e.g. OT,ACT to test)
"""

import os
from pathlib import Path

from pipelines.datasets.au_geoscape_gnaf.utils import clean_all

DATA_DIR = os.path.expanduser(
    os.environ.get("GNAF_DATA_DIR", "~/Downloads/au_geoscape_gnaf_data")
)
ZIP = os.environ.get(
    "GNAF_ZIP", os.path.join(DATA_DIR, "input", "g-naf_may26_gda2020.zip")
)
OUT = os.path.join(DATA_DIR, "output")
SNAPSHOT = os.environ.get("GNAF_SNAPSHOT", "2026-05-01")
_STATES = os.environ.get("GNAF_STATES", "").strip()
STATES = [s.strip() for s in _STATES.split(",") if s.strip()] or None


def main() -> None:
    """Clean every selected state into typed partitioned parquet + dicionario."""
    result = clean_all(
        zip_path=Path(ZIP),
        output_dir=Path(OUT),
        snapshot_date=SNAPSHOT,
        states=STATES,
        stringify=False,
    )
    print(f"snapshot_date: {result['snapshot_date']}")
    print("TOTALS:")
    for t, n in result["counts"].items():
        print(f"  {t}: {n:,}")


if __name__ == "__main__":
    main()
