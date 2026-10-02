"""Uniform access to an EPH wave archive, whatever era it comes from.

INDEC shipped the survey in four different packagings:

  2003 Q3 - 2013 Q4   t<Q><YY>_dta.zip        Stata, inside a plain zip
  2014 Q1 - 2015 Q2   t<Q><YY>_dta.rar        Stata, inside a RAR v4
  2016 Q2 - 2016 Q4   EPH_usu_<N>doTrim_...   semicolon TXT, no subdirectory
  2017 Q1 - 2026 Q1   EPH_usu_<Q>_Trim_...    semicolon TXT, inside a subdirectory

RAR v4 is read with bsdtar (libarchive), which is present on macOS -- no
unrar needed.
"""

import subprocess
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd
import pyreadstat

from models.ar_indec_eph.code.constants import INPUT_DIR

# Which member of the archive is which table. INDEC's member names are not
# consistent across the 87 waves, in four separate ways:
#   case        usu_ / Usu_ / USU_ / usu_Individual_
#   extension   .txt and .txt.txt (2021 Q3, all of 2022, 2020 Q4)
#   prefix      usu_... / EPH_usu_... (2020 Q4)
#   word        "individual" everywhere except 2020 Q4, which says "personas"
# Matching is therefore on a set of substrings of the lowercased basename, and
# data_members asserts exactly one match per table so a future rename fails
# loudly instead of silently dropping a table.
TABLE_HINTS = {
    "microdatos_hogar": ("hogar",),
    "microdatos_individuo": ("individual", "personas"),
}


def archive_path(wave: dict) -> Path:
    return INPUT_DIR / wave["url"].rsplit("/", 1)[-1]


def members(wave: dict) -> list[str]:
    path = archive_path(wave)
    if path.suffix == ".rar":
        out = subprocess.run(
            ["bsdtar", "-tf", str(path)],
            capture_output=True,
            text=True,
            check=True,
        )
        return [m for m in out.stdout.splitlines() if m.strip()]
    with zipfile.ZipFile(path) as zf:
        return [n for n in zf.namelist() if not n.endswith("/")]


def data_members(wave: dict) -> dict[str, str]:
    """Map table key -> member name, for the two data files in the archive.

    Raises if any table matches zero or more than one member, so that a naming
    change in a future wave stops the harvest rather than quietly losing a table.
    """
    suffix = ".dta" if wave["fmt"] == "dta" else ".txt"
    all_members = members(wave)
    candidates = [
        name
        for name in all_members
        if name.rsplit("/", 1)[-1].lower().endswith(suffix)
    ]

    found: dict[str, str] = {}
    for table, hints in TABLE_HINTS.items():
        matches = [
            name
            for name in candidates
            if any(h in name.rsplit("/", 1)[-1].lower() for h in hints)
        ]
        if len(matches) != 1:
            raise RuntimeError(
                f"{wave['year']}Q{wave['quarter']}: expected exactly 1 member for "
                f"{table} matching {hints}, got {matches}. Archive holds {all_members}"
            )
        found[table] = matches[0]
    return found


def extract(wave: dict, member: str, dest_dir: Path) -> Path:
    """Extract one member, flattened, and return the path written."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    out = dest_dir / member.rsplit("/", 1)[-1]
    path = archive_path(wave)
    if path.suffix == ".rar":
        with open(out, "wb") as dst:
            subprocess.run(
                ["bsdtar", "-xOf", str(path), member],
                check=True,
                stdout=dst,
            )
    else:
        with (
            zipfile.ZipFile(path) as zf,
            zf.open(member) as src,
            open(out, "wb") as dst,
        ):
            while chunk := src.read(1 << 20):
                dst.write(chunk)
    return out


def read_dta(path: Path) -> tuple[pd.DataFrame, Any]:
    """Read a Stata file, with the source's column names uppercased.

    Wrapped here for one reason beyond DRY: pyreadstat is annotated as returning
    its own `PandasDataFrame`, which the type checker does not treat as a real
    DataFrame, so every `.columns` and `.astype` on the result is flagged. The
    object genuinely is a DataFrame at runtime, so the conversion is declared
    once here instead of suppressed at each of the four call sites.
    """
    frame, meta = pyreadstat.read_dta(str(path))
    # pyrefly: ignore [bad-argument-type]  pyreadstat's PandasDataFrame is a
    # DataFrame at runtime; its stub is not declared as one.
    frame = pd.DataFrame(frame)
    frame.columns = [str(c).upper() for c in meta.column_names]
    return frame, meta
