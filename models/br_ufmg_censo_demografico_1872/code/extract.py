"""Download the Pop-72 Access database and dump every table to CSV.

The source is a single ~75 MB Jet4 ``.mdb`` inside a ZIP published by
NPHED/Cedeplar. Reading it needs ``mdbtools`` (``brew install mdbtools``);
there is no pure-Python reader in this repo's dependency set.

Run:  uv run models/br_ufmg_censo_demografico_1872/code/extract.py
"""

from __future__ import annotations

import os
import shutil
import subprocess
import zipfile
from pathlib import Path

URL = (
    "http://www.nphed.cedeplar.ufmg.br/wp-content/uploads/2017/06/"
    "Pop-1872-Brasil_versao1_0.zip"
)
MDB_NAME = "Pop-1872-Brasil_versao1_0.mdb"

DATA_DIR = Path(
    os.environ.get(
        "CENSO_1872_DATA",
        Path.home() / "Downloads" / "br_ufmg_censo_demografico_1872_data",
    )
)
INPUT_DIR = DATA_DIR / "input"
CSV_DIR = INPUT_DIR / "csv"

# Access UI metadata, not census data.
SKIP_PREFIXES = ("Switchboard",)


def ensure_mdb() -> Path:
    mdb = INPUT_DIR / MDB_NAME
    if mdb.exists():
        return mdb

    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    archive = INPUT_DIR / "Pop-1872-Brasil_versao1_0.zip"
    if not archive.exists():
        subprocess.run(
            [
                "curl",
                "-fsSL",
                "--retry",
                "3",
                "--max-time",
                "900",
                URL,
                "-o",
                str(archive),
            ],
            check=True,
        )
    with zipfile.ZipFile(archive) as z:
        z.extract(MDB_NAME, INPUT_DIR)
    return mdb


def dump_tables(mdb: Path) -> list[str]:
    if shutil.which("mdb-tables") is None:
        raise RuntimeError(
            "mdbtools not installed -- run: brew install mdbtools"
        )

    listing = subprocess.run(
        ["mdb-tables", "-1", str(mdb)],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    tables = [
        t
        for t in listing.splitlines()
        if t and not t.startswith(SKIP_PREFIXES)
    ]

    CSV_DIR.mkdir(parents=True, exist_ok=True)
    for t in tables:
        csv = CSV_DIR / f"{t.replace(' ', '_')}.csv"
        with csv.open("w") as fh:
            subprocess.run(["mdb-export", str(mdb), t], check=True, stdout=fh)
    return tables


def main() -> None:
    mdb = ensure_mdb()
    tables = dump_tables(mdb)
    print(f"{len(tables)} tables dumped to {CSV_DIR}")


if __name__ == "__main__":
    main()
