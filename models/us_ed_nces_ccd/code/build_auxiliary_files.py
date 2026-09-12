#!/usr/bin/env python3
"""Build the per-table auxiliary-file bundles for us_ed_nces_ccd.

One ZIP per table under ``$CCD_DATA_DIR/auxiliary_files/<table>/``, containing
what a user of *that* table needs to read it: the source's own variable list,
the value labels for its coded columns, and a README recording provenance,
download dates and the transformations applied on load.

The long-form NCES documentation (the per-year file layouts, the CCD data
handbooks, the F-33 form and its instructions) is link-only: those documents are
tens of megabytes, stable at the publisher, and describe the *raw* files rather
than the harmonized panel published here. They are indexed in each README.

Usage:
    uv run --no-project python models/us_ed_nces_ccd/code/build_auxiliary_files.py
"""

from __future__ import annotations

import csv
import datetime as dt
import io
import os
import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

# pyrefly: ignore [missing-import]
import schema

# pyrefly: ignore [missing-import]
from build_artifacts import (
    fetch_varlist,
    parse_values,
)

ROOT = Path(__file__).resolve().parent
ARCH = ROOT / "architecture"
DATA = Path(
    os.environ.get(
        "CCD_DATA_DIR", Path.home() / "Downloads" / "us_ed_nces_ccd_data"
    )
)
AUX = DATA / "auxiliary_files"

DOWNLOAD_DATE = dt.date.today().isoformat()

PORTAL = "https://educationdata.urban.org"

#: Long-form documentation left at the publisher, indexed in every README.
LINK_ONLY = [
    (
        "Education Data Portal documentation and variable definitions",
        f"{PORTAL}/documentation/",
    ),
    (
        "NCES Common Core of Data — data files, per-year layouts and record layouts",
        "https://nces.ed.gov/ccd/files.asp",
    ),
    (
        "NCES Common Core of Data — online documentation and data handbooks",
        "https://nces.ed.gov/ccd/online_documentation.asp",
    ),
    (
        "Census Bureau Annual Survey of School System Finances (F-33) — tables and forms",
        "https://www.census.gov/programs-surveys/school-finances/data/tables.html",
    ),
]

#: Endpoint whose variable list documents each table.
TABLE_ENDPOINT = {
    "school": 24,
    "school_district": 54,
    "school_enrollment": 28,
    "staff": 54,
    "district_finance": 29,
}

CITATION = (
    "Common Core of Data and School District Finance Survey (F-33), Education "
    "Data Portal (Version 0.24.0), Urban Institute, accessed {date}, "
    "{portal}/documentation/, made available under the ODC Attribution License."
)


def _varlist_csv(endpoint_id: int, keep: set[str] | None) -> str:
    """The source's variable list for one endpoint, as CSV."""
    rows = fetch_varlist(endpoint_id)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(
        [
            "source_variable",
            "data_type",
            "label",
            "description",
            "value_labels",
        ]
    )
    for x in sorted(rows, key=lambda r: r["variable"]):
        if keep is not None and x["variable"] not in keep:
            continue
        values = "; ".join(
            f"{k} = {v}" for k, v in parse_values(x.get("values"))
        )
        w.writerow(
            [
                x["variable"],
                x["data_type"],
                schema._unescape(x["label"] or ""),
                schema._unescape(x.get("description") or ""),
                values,
            ]
        )
    return buf.getvalue()


def _dictionary_csv(table_slug: str) -> str:
    rows = [
        r
        for r in csv.DictReader(
            (ARCH / "dicionario_values.csv").open(encoding="utf-8")
        )
        if r["id_tabela"] == table_slug
    ]
    buf = io.StringIO()
    w = csv.DictWriter(
        buf,
        fieldnames=[
            "id_tabela",
            "nome_coluna",
            "chave",
            "cobertura_temporal",
            "valor",
        ],
    )
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue()


def _readme(table: schema.Table, endpoint_id: int) -> str:
    links = "\n".join(f"- {title}\n  {url}" for title, url in LINK_ONLY)
    sentinel_note = (
        "`grade` is the exception: there `-1` is prekindergarten, a real "
        "category, and is preserved.\n"
        if "grade" in {c.name for c in table.columns}
        else ""
    )
    return f"""# us_ed_nces_ccd — `{table.slug}` auxiliary files

{table.desc_en}

## Citation

{CITATION.format(date=DOWNLOAD_DATE, portal=PORTAL)}

The underlying NCES Common Core of Data and Census Bureau F-33 collections are
United States Government works and are in the public domain. The harmonized
republication used here is licensed ODC-By v1.0 and requires attribution.

## Files in this bundle

| File | What it is | Source |
|---|---|---|
| `variable_list.csv` | The source's own variable list for this table — name, type, label, description and value labels | `{PORTAL}/api/v1/api-endpoint-varlist/?endpoint_id={endpoint_id}` |
| `architecture.csv` | The Data Basis column specification: output name, BigQuery type, description, unit, dictionary and directory links, and the source column each is derived from | this repository, `models/us_ed_nces_ccd/code/architecture/{table.slug}.csv` |
| `value_labels.csv` | Every code stored in this table's coded columns and the label it stands for | derived from the variable list |

Downloaded {DOWNLOAD_DATE}.

## What was changed on load

Read this before comparing any figure against the source.

- **`year` is the fall of the school year.** `year = 2020` means school year
  2020-21; for the finance table that is fiscal year 2021.
- **Missing-value codes are NULL.** The source writes `-1` (missing or not
  reported), `-2` (not applicable) and `-3` (suppressed) into every column.
  All three become NULL. {sentinel_note}
- **Identifiers are zero-padded**: `school_id` to 12 characters, `agency_id` to
  7, `state_id` to 2, `county_id` to 5. The source strips leading zeros from
  `leaid` in the enrollment extracts. One malformed 11-character `ncessch`
  (`25000031636`) is corrected to `250000301636`.
- **Codes with significant leading zeros are kept verbatim** — ZIP, ZIP+4,
  CBSA, CSA and the 14-digit Census government id.
- Column names follow Data Basis conventions for English-language datasets:
  snake_case, `_id` suffix on identifiers.

## Further documentation, held at the publisher

{links}
"""


def build(table: schema.Table, endpoint_id: int) -> Path:
    out_dir = AUX / table.slug
    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / "auxiliary_files.zip"

    keep = {c.source for c in table.columns if c.source} or None
    if table.slug == "staff":
        keep = set(schema.STAFF_SOURCE_COLUMNS) | {"leaid", "year", "fips"}

    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("README.md", _readme(table, endpoint_id))
        z.writestr("variable_list.csv", _varlist_csv(endpoint_id, keep))
        z.writestr(
            "architecture.csv",
            (ARCH / f"{table.slug}.csv").read_text(encoding="utf-8"),
        )
        z.writestr("value_labels.csv", _dictionary_csv(table.slug))
    return dest


def main() -> None:
    header = next(
        csv.reader(
            (DATA / "input" / "districts_ccd_finance.csv").open(
                encoding="utf-8"
            )
        )
    )
    labels = {x["variable"]: x["label"] for x in fetch_varlist(29)}
    tables = [*schema.STATIC_TABLES, schema.finance_table(header, labels)]

    for table in tables:
        dest = build(table, TABLE_ENDPOINT[table.slug])
        print(
            f"  {table.slug}: {dest.stat().st_size / 1024:,.0f} KB -> {dest}"
        )
    print(
        f"\nUpload with:\n  gsutil -m cp -r {AUX}/* gs://basedosdados/auxiliary_files/{schema.DATASET}/"
    )


if __name__ == "__main__":
    main()
