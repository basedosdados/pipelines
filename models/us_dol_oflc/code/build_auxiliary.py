"""Build the per-table auxiliary-file bundles for us_dol_oflc.

Each program table gets one ZIP holding every record-layout document the
Department of Labor published for that program, plus the committed crosswalk
for that table and a README that says where each file came from, when it was
downloaded, and what the cleaning code does to the data.

The record layouts are the only way to read the older files: the column set
changes almost every fiscal year, and the layouts are what the crosswalk was
checked against.

Usage:
    uv run python models/us_dol_oflc/code/build_auxiliary.py
"""

from __future__ import annotations

import datetime as dt
import os
import re
import shutil
import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
DATA = Path(
    os.environ.get("OFLC_DATA_DIR", Path.home() / "Downloads/us_dol_oflc_data")
)
LAYOUTS = DATA / "input" / "layouts"
OUT = DATA / "auxiliary_files"
BASE_URL = "https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/"
PAGE = "https://www.dol.gov/agencies/eta/foreign-labor/performance"

# Which layout documents belong to which table.
MATCH = {
    "lca": re.compile(r"^(H-1B|H1B|LCA)_", re.I),
    "perm": re.compile(r"^PERM", re.I),
    "h2a": re.compile(r"^(H-2A|H2A)", re.I),
    "h2b": re.compile(r"^(H-2B|H2B)", re.I),
}

PROGRAM_NAME = {
    "lca": "Labor Condition Application (H-1B, H-1B1, E-3)",
    "perm": "PERM permanent labor certification (ETA-9089)",
    "h2a": "H-2A temporary agricultural labor certification",
    "h2b": "H-2B temporary non-agricultural labor certification",
}

README = """# us_dol_oflc — {table} auxiliary files

Supporting documents for the `{table}` table of the Data Basis dataset
`us_dol_oflc`: {program}.

## Citation

U.S. Department of Labor, Employment and Training Administration, Office of
Foreign Labor Certification. Foreign Labor Certification performance data.
<{page}>

Materials created by the U.S. federal government are in the public domain and
may be used, reproduced and distributed without permission
(<https://www.dol.gov/general/aboutdol/copyright>). Credit is given to the
U.S. Department of Labor.

## What is in this bundle

### Record layouts

The Department of Labor publishes one record-layout document per program per
fiscal year, naming and defining every column in that year's disclosure file.
They are reproduced here unchanged; each was downloaded on {date} from
`{base}<filename>`.

{files}

### Crosswalk

`{table}_crosswalk.csv` is the Data Basis year-to-canonical column map for this
table. One row per source column per source file, with the canonical column it
became, or the reason it was not published. Every column of every source file
appears exactly once, so nothing is dropped silently.

## What the cleaning code does

- **`year` is the federal FISCAL year** (1 October to 30 September), taken from
  the source file, not from any date in the row.
- **One row per case number per fiscal year.** Where a fiscal year is published
  in more than one file — the two FY2009 LCA systems, the two FY2024 PERM form
  versions, the quarterly LCA files from FY2020 — the files are unioned and, in
  the rare case of a repeated case number, the last file read wins.
- **Only the primary worksite is published.** Applications covering several
  worksites list the rest in companion Appendix and Addendum files, which are
  not part of this table.
- **Wages keep the source pair and gain a derived annual column.** The source
  reports an amount next to a separate unit of pay. The unit is harmonised to
  hour, day, week, bi-weekly, semi-monthly, month, year or piece rate; the
  annualised column multiplies the amount by 2080, 260, 52, 26, 24, 12 or 1
  respectively. A piece rate has no period, so it never annualises, and a
  missing or unrecognised unit leaves the annualised column NULL rather than
  assuming a period.
- **Personal contact details of individuals are not published** — employer
  points of contact, attorneys and preparers. Business names are kept. No
  beneficiary of a certification is named anywhere in the source files.
"""


def main() -> int:
    if not LAYOUTS.exists():
        raise SystemExit(f"Missing record layouts at {LAYOUTS}")
    OUT.mkdir(parents=True, exist_ok=True)
    date = dt.date.today().isoformat()
    for table, rx in MATCH.items():
        docs = sorted(p for p in LAYOUTS.iterdir() if rx.match(p.name))
        if not docs:
            raise SystemExit(f"No record layouts matched {table}")
        listing = "\n".join(
            f"- `{p.name}` ({p.stat().st_size // 1024} KB)" for p in docs
        )
        tdir = OUT / table
        tdir.mkdir(parents=True, exist_ok=True)
        readme = README.format(
            table=table,
            program=PROGRAM_NAME[table],
            page=PAGE,
            base=BASE_URL,
            date=date,
            files=listing,
        )
        (tdir / "README.md").write_text(readme)
        shutil.copy(
            HERE / "crosswalk" / f"{table}.csv",
            tdir / f"{table}_crosswalk.csv",
        )
        zpath = OUT / f"{table}_auxiliary_files.zip"
        with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED) as z:
            z.write(tdir / "README.md", "README.md")
            z.write(tdir / f"{table}_crosswalk.csv", f"{table}_crosswalk.csv")
            for p in docs:
                z.write(p, f"record_layouts/{p.name}")
        print(
            f"{table}: {len(docs)} layouts -> {zpath} "
            f"({zpath.stat().st_size / 1e6:.1f} MB)"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
