"""Build one auxiliary-file bundle per table for us_dot_fars.

Per ``.claude/rules/auxiliary-files.md`` each bundle is a ZIP holding the
documents a user of *that table* needs in hand, plus a README recording the
citation, per-file provenance and download date. Long-form PDFs that are large
and stable at the publisher are listed as links rather than rehosted.

What goes in:

* the per-year SAS ``PROC FORMAT`` source, which is NHTSA's own machine-readable
  code -> label map and the only such map for 1975-2014. It is the raw material
  behind the ``dicionario`` table, bundled so a user can verify that table rather
  than take it on trust.
* the per-year release notes, which record what each annual and Annual Report
  File release changed.

What stays a link: the FARS Analytical User's Manual (9.5 MB) and the vPIC
manual, both stable at crashstats.nhtsa.dot.gov.

Run: uv run python models/us_dot_fars/code/build_auxiliary_files.py
"""

import shutil
import zipfile
from datetime import date
from pathlib import Path

import requests
from common import DATA_TABLES, DATASET_ID, INPUT, OUTPUT

BUNDLE_DIR = OUTPUT.parent / "auxiliary_files"
TODAY = date.today().isoformat()

RELEASE_NOTES_URL = "https://static.nhtsa.gov/nhtsa/downloads/FARS/{year}/FARS{year}%20Release%20Notes.txt"
MANUALS_INDEX_URL = "https://static.nhtsa.gov/nhtsa/downloads/FARS/Links%20for%20FARS%20Manuals.pdf"
USER_MANUAL_URL = (
    "https://crashstats.nhtsa.dot.gov/Api/Public/ViewPublication/813794"
)
VPIC_MANUAL_URL = (
    "https://crashstats.nhtsa.dot.gov/Api/Public/ViewPublication/813697"
)

SOURCE_FILE = {"crash": "accident", "vehicle": "vehicle", "person": "person"}

README = """# FARS auxiliary files — table `{table}`

Fatality Analysis Reporting System (FARS), National Highway Traffic Safety
Administration, U.S. Department of Transportation.

Suggested citation:
  National Highway Traffic Safety Administration. Fatality Analysis Reporting
  System (FARS), 1975-{last}. Washington, DC: U.S. Department of Transportation.

Data are a work of the U.S. federal government and are in the public domain.

## What this table is

`{table}` holds {grain}. It is built from the `{source}.csv` member of NHTSA's
annual national CSV release, one file per year from 1975 to {last}.

## Files in this bundle

* `formats/format_<year>.sas` — NHTSA's own `PROC FORMAT` source for that year,
  giving the meaning of every coded value. Downloaded {today} from the SAS
  release at
  `https://static.nhtsa.gov/nhtsa/downloads/FARS/<year>/National/FARS<year>NationalSAS.zip`.
  These files cover 1975-2014. From 2015 the CSVs carry a `<VAR>NAME` label
  column beside every coded column, so the labels come from the data itself and
  no separate format file is needed.

* `release_notes/FARS<year>_release_notes.txt` — what each release changed,
  including which years the Annual Report File revised. Downloaded {today} from
  `https://static.nhtsa.gov/nhtsa/downloads/FARS/<year>/FARS<year> Release Notes.txt`.

## Documents linked rather than bundled

* FARS Analytical User's Manual, 1975-{last} (9.5 MB PDF) —
  {manual}
  The authoritative per-year description of every variable and code set.
* Product Information Catalog and Vehicle Listing (vPIC) Analytical User's
  Manual — {vpic}
* Index of all FARS manuals — {index}

## What you must know to read this table

1. **Codes are not stable over time.** Nearly every coded column had its code set
   redefined at least once between 1975 and {last} — light condition four times,
   vehicle body type eleven. Read a code against the `dicionario` table on the
   pair (code, `cobertura_temporal`), not on the code alone. Pooling a coded
   column across the full span without regard to era will mix categories.

2. **Coded columns are STRING on purpose.** FARS sentinels are dense integers
   sitting in the same range as real values (99 = unknown, 996 = test not given),
   so casting a coded column to an integer silently turns "unknown" into a
   number.

3. **Sentinels have been converted to NULL in the numeric columns**, per year and
   per variable, using the format files in this bundle. Two cases are worth
   naming: `age`, whose unknown code moved from 99 to 998/999 in 2009, and
   `blood_alcohol_content`, which is derived from the source code by dividing by
   100 through 2014 and by 1000 from 2015. The raw code is preserved in
   `alcohol_test_result_code` so no information is lost.

4. **Latitude and longitude** are published from 1999 but are of uneven quality
   before roughly 2001. Filler values (77.7777, 88.8888, 99.9999) are NULL here.

5. **Coverage is the 50 states and the District of Columbia.** Puerto Rico and
   the other territories are published in separate FARS files and are not
   included.
"""

GRAIN = {
    "crash": "one row per fatal crash, keyed by year, state and case number",
    "vehicle": "one row per vehicle involved in a fatal crash",
    "person": "one row per person involved in a fatal crash, occupant or not",
}


def _format_sources(work: Path, years: range) -> None:
    out = work / "formats"
    out.mkdir(parents=True, exist_ok=True)
    for year in years:
        src = INPUT / f"FARS{year}SAS.zip"
        if not src.exists():
            continue
        with zipfile.ZipFile(src) as zf:
            sas = [n for n in zf.namelist() if n.lower().endswith(".sas")]
            if not sas:
                continue
            text = "".join(zf.read(n).decode("latin-1") for n in sas)
        (out / f"format_{year}.sas").write_text(text, encoding="utf-8")


def _release_notes(
    work: Path, years: range, session: requests.Session
) -> None:
    out = work / "release_notes"
    out.mkdir(parents=True, exist_ok=True)
    for year in years:
        try:
            r = session.get(RELEASE_NOTES_URL.format(year=year), timeout=120)
        except requests.RequestException:
            continue
        if r.status_code == 200 and r.content:
            (out / f"FARS{year}_release_notes.txt").write_bytes(r.content)


def main() -> None:
    last = max(
        int(p.name.split("=")[1]) for p in (OUTPUT / "crash").glob("year=*")
    )
    years = range(1975, last + 1)
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    BUNDLE_DIR.mkdir(parents=True, exist_ok=True)
    staging = BUNDLE_DIR / "_staging"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)

    _format_sources(staging, years)
    _release_notes(staging, years, session)
    print(
        f"collected {len(list((staging / 'formats').glob('*.sas')))} format files, "
        f"{len(list((staging / 'release_notes').glob('*.txt')))} release notes"
    )

    for table in DATA_TABLES:
        target = BUNDLE_DIR / table
        target.mkdir(parents=True, exist_ok=True)
        zip_path = target / "auxiliary_files.zip"
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(
                "README.md",
                README.format(
                    table=table,
                    grain=GRAIN[table],
                    source=SOURCE_FILE[table],
                    last=last,
                    today=TODAY,
                    manual=USER_MANUAL_URL,
                    vpic=VPIC_MANUAL_URL,
                    index=MANUALS_INDEX_URL,
                ),
            )
            for f in sorted(staging.rglob("*")):
                if f.is_file():
                    zf.write(f, f.relative_to(staging).as_posix())
        size = zip_path.stat().st_size / 1e6
        print(f"  {table}: {zip_path} ({size:.1f} MB)")

    print(
        "\nUpload each bundle to:\n"
        "  gs://basedosdados/auxiliary_files/"
        f"{DATASET_ID}/<table>/auxiliary_files.zip\n"
        "and set auxiliary_files_url on the table. Note the bucket is "
        "requester-pays, so the published URL currently returns HTTP 400 for an "
        "anonymous fetch — verify and report the real status rather than "
        "assuming the link resolves."
    )


if __name__ == "__main__":
    main()
