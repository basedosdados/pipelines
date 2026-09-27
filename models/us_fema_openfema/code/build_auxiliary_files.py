"""Build the per-table auxiliary-file bundles for us_fema_openfema.

OpenFEMA publishes no codebook PDF: its documentation *is* the machine-readable
field dictionary behind the API, plus a per-set data page on fema.gov. So each
bundle carries that set's full field dictionary as a CSV — FEMA's own
descriptions at full length, where the catalog carries a condensed one-line
version — together with the dictionary of coded values for that table and a
README recording provenance, the terms of use and FEMA's mandatory disclaimer.

Bundles are written to ``<output>/auxiliary_files/<table>/auxiliary_files.zip``
for a separate upload step. Stdlib only, so it can run while another process is
using the shared virtualenv.

    python3 models/us_fema_openfema/code/build_auxiliary_files.py <output_dir>
"""

from __future__ import annotations

import csv
import io
import json
import sys
import zipfile
from datetime import date
from pathlib import Path

HERE = Path(__file__).resolve().parent

RETRIEVED = date.today().isoformat()

DISCLAIMER = (
    "This product uses the Federal Emergency Management Agency's OpenFEMA "
    "API, but is not endorsed by FEMA. The Federal Government or FEMA cannot "
    "vouch for the data or analyses derived from these data after the data "
    "have been retrieved from the Agency's website(s)."
)

TERMS_URL = "https://www.fema.gov/about/openfema/terms-conditions"

# table slug -> (OpenFEMA set name, version, data page)
TABLES = {
    "disaster_declaration": (
        "DisasterDeclarationsSummaries",
        2,
        "https://www.fema.gov/openfema-data-page/"
        "disaster-declarations-summaries-v2",
    ),
    "public_assistance_project": (
        "PublicAssistanceFundedProjectsDetails",
        2,
        "https://www.fema.gov/openfema-data-page/"
        "public-assistance-funded-projects-details-v2",
    ),
    "nfip_claim": (
        "NfipClaims",
        3,
        "https://www.fema.gov/openfema-data-page/nfip-redacted-claims-v3",
    ),
    "nfip_policy": (
        "NfipPolicies",
        3,
        "https://www.fema.gov/openfema-data-page/nfip-redacted-policies-v3",
    ),
}

README = """# Auxiliary files — `{gcp_dataset_id}.{table}`

Source: OpenFEMA data set **{set_name}** v{version}, published by the U.S.
Federal Emergency Management Agency.

- Data page: {data_page}
- API endpoint: {web_service}
- Bulk file used for this table: {access_url}
- Retrieved: {retrieved}
- Records at retrieval: {record_count}
- Source coverage: {temporal}
- Publication cadence: {periodicity}

## Citation

FEMA requires the endpoint and the retrieval date to be cited:

> Federal Emergency Management Agency (FEMA), OpenFEMA Dataset:
> {title} - v{version}. Retrieved from {data_page} on {retrieved}.
> {disclaimer}

## Terms of use — read before redistributing

This data set is **not** in the public domain. It is governed by the OpenFEMA
terms and conditions, {terms_url}, which bind every user to, among other
things:

- use the data **solely for statistical research or as a reporting record**;
- **not** use it to make determinations that might affect an individual's
  rights or eligibility for benefits;
- not reidentify, nor attempt to reidentify, the individuals whose data is
  aggregated, and not publish facts that may lead to their identification;
- cease use and destroy any copy if FEMA requests it.

FEMA and DHS logos and seals may not be used without written authorisation.

## What is in this bundle

| file | what it is |
|---|---|
| `field_dictionary.csv` | Every field OpenFEMA publishes for this set, with FEMA's own full-length description, type, nullability and sort order. The Data Basis catalog carries a condensed one-line version of the same text. |
| `value_dictionary.csv` | Value-to-label legend for every coded column in this table, as loaded into the `dicionario` table. |
| `README.md` | This file. |

## How the Data Basis table differs from the source file

- Column names are snake_case English. `original_name` in the field dictionary
  gives the source spelling for every column.
- Columns whose stored values are opaque codes are typed STRING and covered by
  `value_dictionary.csv`, whatever the source's numeric storage.
- FEMA's internal bookkeeping columns (`hash`, `lastRefresh`) are dropped.
- A `year` partition column is derived from {partition_source}.
- Geographic identifiers are normalised: a five-digit `county_id`, and — for
  the NFIP tables — an eleven-digit `census_tract_id` derived from the source's
  twelve-digit block-group `census_geoid`. County codes meaning "statewide" are
  NULL rather than a county that does not exist.
{table_notes}
## Redaction

{redaction}
"""

NFIP_REDACTION = """FEMA publishes these as the *redacted* claims and policies files, and this
bundle republishes exactly what FEMA published. Latitude and longitude arrive
already rounded to one decimal place, and the address is reduced to city,
postal code and census block group. Nothing here has been geocoded, joined to
a finer geography, or otherwise sharpened, and doing so would be the
reidentification the terms forbid."""

PLAIN_REDACTION = """This set describes declarations and grant-funded projects rather than
individuals, and FEMA applies no redaction to it. It is republished as
published."""

NOTES = {
    "public_assistance_project": (
        "- The source emits FEMA-internal codes 1001 and 1003 for the Northern\n"
        "  Mariana Islands and American Samoa rather than their FIPS codes 69\n"
        "  and 60; they are corrected here. Neither the state nor the county\n"
        "  code is zero-padded at source; both are padded before use.\n"
    ),
    "nfip_claim": (
        "- `reported_zip_code` is normalised to five digits: empty strings\n"
        "  become NULL and the 167 ZIP+4 values are truncated.\n"
    ),
    "nfip_policy": (
        "- The source carries no county column; `county_id` is derived from the\n"
        "  first five digits of `census_geoid`.\n"
    ),
}


def field_dictionary(meta: dict, set_name: str, version: int) -> bytes:
    rows = sorted(
        (
            f
            for f in meta["fields"]
            if f["openFemaDataSet"] == set_name
            and f["datasetVersion"] == version
        ),
        key=lambda f: f["sortOrder"],
    )
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(
        [
            "sort_order",
            "original_name",
            "title",
            "type",
            "is_nullable",
            "is_primary_key",
            "description",
        ]
    )
    for f in rows:
        writer.writerow(
            [
                f["sortOrder"],
                f["name"],
                f["title"],
                f["type"],
                str(f["isNullable"]).lower(),
                str(f["primaryKey"]).lower(),
                " ".join((f["description"] or "").split()),
            ]
        )
    return buffer.getvalue().encode()


def value_dictionary(table: str) -> bytes:
    with (HERE / "dicionario.csv").open() as handle:
        rows = [r for r in csv.DictReader(handle) if r["id_tabela"] == table]
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(["column", "code", "label"])
    for row in rows:
        writer.writerow([row["nome_coluna"], row["chave"], row["valor"]])
    return buffer.getvalue().encode()


def main(output_dir: Path) -> None:
    meta = json.loads((HERE / "source_metadata.json").read_text())
    catalog = {d["name"]: d for d in meta["datasets"]}
    sys.path.insert(0, str(HERE.parents[2]))
    from pipelines.datasets.us_fema_openfema.spec import TABLES as SPEC

    root = output_dir / "auxiliary_files"
    for table, (set_name, version, data_page) in TABLES.items():
        entry = catalog[set_name]
        access_url = next(
            d["accessURL"]
            for d in entry["distribution"]
            if d["format"] == "parquet"
        )
        readme = README.format(
            gcp_dataset_id="us_fema_openfema",
            table=table,
            set_name=set_name,
            version=version,
            title=entry["title"],
            data_page=data_page,
            web_service=entry["webService"],
            access_url=access_url,
            retrieved=RETRIEVED,
            record_count=f"{entry['recordCount']:,}",
            temporal=entry.get("temporal") or "not stated",
            periodicity=entry.get("accrualPeriodicity") or "not stated",
            terms_url=TERMS_URL,
            disclaimer=DISCLAIMER,
            partition_source=SPEC[table]["partition_source"],
            table_notes=NOTES.get(table, ""),
            redaction=(
                NFIP_REDACTION if table.startswith("nfip") else PLAIN_REDACTION
            ),
        )
        target = root / table
        target.mkdir(parents=True, exist_ok=True)
        bundle = target / "auxiliary_files.zip"
        with zipfile.ZipFile(bundle, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("README.md", readme)
            zf.writestr(
                "field_dictionary.csv",
                field_dictionary(meta, set_name, version),
            )
            zf.writestr("value_dictionary.csv", value_dictionary(table))
        print(f"{table:<28} {bundle.stat().st_size:>8,} bytes")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    main(Path(sys.argv[1]).expanduser())
