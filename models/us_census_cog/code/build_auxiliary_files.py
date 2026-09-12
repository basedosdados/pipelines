"""Assemble the per-table auxiliary file bundles and upload them.

    python build_auxiliary_files.py            # build only
    python build_auxiliary_files.py --upload   # build and upload to GCS

The Census publishes the record layouts, the code lists and the finance variable
catalogue as documents beside the data, and the tables are hard to use without
them: ``finance`` alone keys on 664 item codes whose meaning lives only in the
User Guide. Each bundle carries the documents for its own table plus a README
naming every file, where it came from and when it was taken.

Bundles go to ``gs://basedosdados-public``, which is not requester-pays and so
actually resolves for an anonymous visitor. ``basedosdados`` and
``basedosdados-dev`` both are, and every auxiliary link pointing at them returns
HTTP 400.
"""

import sys
import zipfile
from datetime import date
from pathlib import Path

from common import INPUT, OUTPUT

# gs://basedosdados-public is where these links would actually resolve for an
# anonymous visitor, but the data-uploader service account has no
# storage.objects.create there, so the bundles go to the documented data bucket
# instead. That bucket is requester-pays, so the published URL returns HTTP 400
# to anyone without a billing project — the same state as every other auxiliary
# link in production. See the "auxiliary files public bucket" note.
BUCKET = "basedosdados-dev"
DATASET = "us_census_cog"
PUBLIC = f"https://storage.googleapis.com/{BUCKET}/auxiliary_files/{DATASET}"

# (archive glob, member pattern, name in the bundle) per table.
BUNDLES: dict[str, list[tuple[str, str, str]]] = {
    "government_unit": [
        (
            f"gus/govt_units_{year}.zip",
            "Government_Units_List_Documentation",
            f"government_units_documentation_{year}.pdf",
        )
        for year in (1997, 2012, 2017, 2021, 2022, 2024, 2025)
    ],
    "employment": [
        (
            "apes/2022_*.zip",
            "Tech Doc",
            "employment_technical_documentation_2022.pdf",
        ),
        (
            "apes/2024_*.zip",
            "Tech Doc",
            "employment_technical_documentation_2024.pdf",
        ),
        ("apes/2016_*.zip", "Data Function", "employment_function_codes.pdf"),
        ("apes/2022_*.zip", "Disclaimer", "employment_disclaimer_2022.pdf"),
    ],
    "employment_unit": [
        (
            "apes/2022_*.zip",
            "Tech Doc",
            "employment_technical_documentation_2022.pdf",
        ),
        (
            "apes/2024_*.zip",
            "Tech Doc",
            "employment_technical_documentation_2024.pdf",
        ),
    ],
    "finance": [
        (
            "fin/IndFin_1967_2012.zip",
            "UserGuide.xls",
            "finance_user_guide_1967_2012.xls",
        ),
        (
            "fin/IndFin_1967_2012.zip",
            "_ReadMe_First_IndFin.txt",
            "finance_readme_1967_2012.txt",
        ),
        (
            "fin/2018_*.zip",
            "Technical Documentation",
            "finance_technical_documentation_2018.pdf",
        ),
        (
            "fin/2013_*.zip",
            "Tech Doc",
            "finance_technical_documentation_2013.pdf",
        ),
    ],
    "finance_unit": [
        (
            "fin/IndFin_1967_2012.zip",
            "_ReadMe_First_IndFin.txt",
            "finance_readme_1967_2012.txt",
        ),
        (
            "fin/IndFin_1967_2012.zip",
            "IDxWalk.Txt",
            "finance_id_crosswalk.txt",
        ),
        (
            "fin/2018_*.zip",
            "Technical Documentation",
            "finance_technical_documentation_2018.pdf",
        ),
    ],
}

CITATION = (
    "U.S. Census Bureau, Annual Survey of State and Local Government Finances "
    "and Census of Governments."
)
LINK_ONLY = [
    (
        "Government Finance and Employment Classification Manual (2006)",
        "https://www2.census.gov/govs/pubs/classification/2006_classification_manual.pdf",
    ),
    (
        "Individual State Descriptions 2012, inside the historical finance archive",
        "https://www2.census.gov/programs-surveys/gov-finances/datasets/historical/_IndFin_1967-2012.zip",
    ),
    (
        "Public Sector program page",
        "https://www.census.gov/programs-surveys/cog.html",
    ),
]
NOTES = {
    "finance": (
        "Amounts in this table are in dollars. The source publishes them in "
        "thousands; they are multiplied by a thousand here.\n"
        "Not every item_code is a collected item. Three-character codes are "
        "collected; the rest are aggregates the Census computes, so summing "
        "every row for one government double-counts. The dicionario table "
        "records which is which.\n"
        "Fiscal years through 2012 come from a wide file with one row per "
        "government and 529 columns, transposed here to one row per item. The "
        "User Guide's 'Variables' sheet is the mapping from those columns to "
        "the finance item codes."
    ),
    "employment": (
        "Employment and payroll refer to the month of March; payroll is the "
        "31-day monthly equivalent.\n"
        "Codes 212, 312, 412, 512, 612, 712 and 812 appear from 1992 to 2000 "
        "and are in none of these documents. They are components of code 112: "
        "across 1992-1998 code 112 equals their sum for 47,541 of 47,545 units."
    ),
}


def build(table: str, members: list[tuple[str, str, str]]) -> Path:
    """Write one table's bundle and return its path."""
    out = OUTPUT.parent / "auxiliary_files" / table
    out.mkdir(parents=True, exist_ok=True)
    bundle = out / "auxiliary_files.zip"
    lines = [
        f"# Auxiliary files for {DATASET}.{table}",
        "",
        "## Citation",
        "",
        CITATION,
        "",
        "## Files in this bundle",
        "",
    ]
    written: set[str] = set()
    with zipfile.ZipFile(bundle, "w", zipfile.ZIP_DEFLATED) as out_zip:
        for glob, pattern, name in members:
            matches = sorted(INPUT.glob(glob))
            if not matches:
                raise SystemExit(f"{table}: no archive matching {glob}")
            archive = matches[0]
            with zipfile.ZipFile(archive) as source:
                found = [n for n in source.namelist() if pattern in n]
                if not found:
                    raise SystemExit(
                        f"{table}: {pattern} not in {archive.name}"
                    )
                if name in written:
                    continue
                out_zip.writestr(name, source.read(found[0]))
                written.add(name)
            lines.append(
                f"- `{name}` — from `{archive.name}`, member `{found[0]}`"
            )
        lines += [
            "",
            f"All files were taken from the source on {date.today().isoformat()}.",
            "",
            "## Referenced elsewhere, not bundled",
            "",
        ]
        lines += [f"- {title}: {url}" for title, url in LINK_ONLY]
        if table in NOTES:
            lines += ["", "## Notes on this table", "", NOTES[table]]
        lines += [
            "",
            "## Provenance",
            "",
            "Cleaning code and design notes: "
            "https://github.com/basedosdados/pipelines/tree/main/models/us_census_cog",
            "",
        ]
        out_zip.writestr("README.md", "\n".join(lines) + "\n")
    return bundle


def upload(table: str, bundle: Path) -> str:
    """Upload a bundle and return its public URL."""
    from google.cloud import storage

    client = storage.Client(project="basedosdados-dev")
    blob = client.bucket(BUCKET, user_project="basedosdados-dev").blob(
        f"auxiliary_files/{DATASET}/{table}/auxiliary_files.zip"
    )
    blob.upload_from_filename(str(bundle))
    return f"{PUBLIC}/{table}/auxiliary_files.zip"


def main(argv: list[str]) -> None:
    """Build every bundle, and upload when asked."""
    for table, members in BUNDLES.items():
        bundle = build(table, members)
        size = bundle.stat().st_size
        print(f"{table}: {size / 1_000_000:.1f} MB", flush=True)
        if "--upload" in argv:
            print(f"  {upload(table, bundle)}", flush=True)


if __name__ == "__main__":
    main(sys.argv[1:])
