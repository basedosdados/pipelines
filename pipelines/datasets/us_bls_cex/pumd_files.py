"""Locate the CE PUMD quarter files inside the downloaded release zips.

Each annual release zip (intrvwYY.zip, diaryYY.zip) holds one CSV per file
family and collection quarter, named like ``fmli242.csv`` (2024 Q2) or
``fmli241x.csv``. Folder layout inside the zips is inconsistent across years
(``diary96/diary96/``, ``expn96/`` at the root), so files are matched by name.

Before 2020 an interview release carries five quarters, so the first quarter of
a year appears in two releases: as ``YY1`` in the previous year's zip and as
``YY1x`` in its own year's zip. From 2020 on each quarter ships once. We keep
exactly one file per (family, year, quarter), preferring the release whose year
equals the collection year (the reprocessed ``x`` file), else the only one.
"""

import os
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path

DATA_DIR = Path(
    os.environ.get(
        "US_BLS_CEX_DATA_DIR",
        Path.home() / "Library" / "Caches" / "us_bls_cex_data",
    )
)
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"
PUMD_DIR = INPUT_DIR / "pumd"
DOCS_DIR = INPUT_DIR / "docs"
LABSTAT_DIR = INPUT_DIR / "labstat"
DICTIONARY_XLSX = DOCS_DIR / "ce-pumd-interview-diary-dictionary.xlsx"

FIRST_RELEASE = 1996
LAST_RELEASE = 2024

# file family -> (survey, table slug)
FAMILIES = {
    "fmli": ("interview", "interview_household"),
    "memi": ("interview", "interview_member"),
    "mtbi": ("interview", "interview_expenditure"),
    "itbi": ("interview", "interview_income"),
    "fmld": ("diary", "diary_household"),
    "memd": ("diary", "diary_member"),
    "expd": ("diary", "diary_expenditure"),
    "dtbd": ("diary", "diary_income"),
}

_MEMBER = re.compile(r"(?:^|/)([a-z]{4})(\d{2})(\d)(x?)\.csv$", re.IGNORECASE)


def full_year(yy: str) -> int:
    y = int(yy)
    return 1900 + y if y >= 80 else 2000 + y


@dataclass(frozen=True)
class QuarterFile:
    family: str
    year: int  # collection year
    quarter: int  # collection quarter, 1-4
    release: int  # year of the release zip it came from
    zip_path: Path
    member: str

    @property
    def is_x(self) -> bool:
        return self.member.lower().endswith("x.csv")


def release_zips(release: int) -> list[Path]:
    yy = f"{release % 100:02d}"
    return [PUMD_DIR / f"intrvw{yy}.zip", PUMD_DIR / f"diary{yy}.zip"]


def all_quarter_files() -> list[QuarterFile]:
    """Every family quarter file in every release zip, duplicates included."""
    found = []
    for release in range(FIRST_RELEASE, LAST_RELEASE + 1):
        for zp in release_zips(release):
            with zipfile.ZipFile(zp) as zf:
                for member in zf.namelist():
                    m = _MEMBER.search(member)
                    if not m or m.group(1).lower() not in FAMILIES:
                        continue
                    found.append(
                        QuarterFile(
                            family=m.group(1).lower(),
                            year=full_year(m.group(2)),
                            quarter=int(m.group(3)),
                            release=release,
                            zip_path=zp,
                            member=member,
                        )
                    )
    return found


def selected_quarter_files() -> list[QuarterFile]:
    """One file per (family, year, quarter); see the module docstring."""
    best: dict[tuple[str, int, int], QuarterFile] = {}
    for qf in all_quarter_files():
        key = (qf.family, qf.year, qf.quarter)
        cur = best.get(key)
        if cur is None or (qf.release == qf.year and cur.release != cur.year):
            best[key] = qf
    return sorted(best.values(), key=lambda q: (q.family, q.year, q.quarter))


def read_header(qf: QuarterFile) -> list[str]:
    with zipfile.ZipFile(qf.zip_path) as zf, zf.open(qf.member) as fh:
        line = fh.readline().decode("latin-1").strip()
    return [h.strip().strip('"').lower() for h in line.split(",")]
