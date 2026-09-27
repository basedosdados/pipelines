"""Assemble the per-table auxiliary-file bundles for us_fbi_cde.

The FBI publishes the documentation a user of these tables needs in order to
read them at all — the NIBRS data dictionary and entity diagram, the Return A
and Police Employee record descriptions, and the hate crime methodology. Each
bundle holds only the documents for its own table, plus a README naming the
citation, the provenance of each file and the date it was fetched.

Bundles are written locally; ``--upload`` pushes them to the documented GCS
path. Note that both data buckets are requester-pays, so the published URLs
return HTTP 400 for an anonymous visitor until the pending migration to
``gs://basedosdados-public`` lands. The script reports the real status of every
URL rather than assuming it resolves.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import urllib.request
import zipfile
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_fbi_cde.constants import constants
from pipelines.datasets.us_fbi_cde.utils import signed_url

DATA_ROOT = constants.DATA_ROOT.value
INPUT = DATA_ROOT / "input"
BUNDLE_ROOT = DATA_ROOT / "auxiliary_files"
BUCKET = "basedosdados"
TODAY = date.today().isoformat()

CITATION = (
    "Federal Bureau of Investigation, Uniform Crime Reporting Program. "
    "Crime Data Explorer. https://cde.ucr.cjis.gov/"
)

# Documents fetched from the CDE, keyed by the name they take in the bundle.
SOURCES = {
    "nibrs_data_dictionary.pdf": "nibrs/_all/NIBRS_DataDictionary.pdf",
    "nibrs_entity_diagram.pdf": "nibrs/_all/nibrs_diagram.pdf",
    "return_a_record_description.zip": "master_files/reta/reta-help.zip",
    "police_employee_record_description.zip": "master_files/pe/pe-help.zip",
}

NIBRS_TABLES = [
    "incident",
    "offense",
    "offender",
    "victim",
    "victim_offense",
    "victim_offender_relationship",
    "arrestee",
    "property",
]

# {table: [(bundle filename, note)]}
BUNDLES: dict[str, list[tuple[str, str]]] = {}
for _table in NIBRS_TABLES:
    BUNDLES[_table] = [
        (
            "nibrs_data_dictionary.pdf",
            "Definition of every NIBRS data element, its permitted values and the "
            "rules governing when it must be reported.",
        ),
        (
            "nibrs_entity_diagram.pdf",
            "Entity relationship diagram of the NIBRS segments, showing how "
            "incidents, offenses, offenders, victims, property and arrestees join.",
        ),
    ]
BUNDLES["agency"] = [
    (
        "police_employee_record_description.zip",
        "Record description for the Law Enforcement Employees master file, which "
        "supplies this table's officer and civilian counts.",
    ),
    (
        "return_a_record_description.zip",
        "Record description for the Return A master file, which supplies "
        "summary_months_reported, covered_by_ori and the officer assault counts.",
    ),
]
BUNDLES["ucr_summary"] = [
    (
        "return_a_record_description.zip",
        "Record description for the Return A master file: the fixed-width layout "
        "this table is parsed from, and the table of overpunch characters used to "
        "encode negative adjustment entries.",
    ),
]
BUNDLES["hate_crime"] = [
    (
        "hate_crime_methodology.pdf",
        "The FBI's methodology note for the hate crime series, including how bias "
        "motivation is recorded and what the multiple-bias flag means.",
    ),
]

# Long-form documents that stay at the publisher: large, stable and rarely
# opened, so they are linked rather than rehosted.
LINK_ONLY = [
    (
        "NIBRS User Manual",
        "https://le.fbi.gov/file-repository/ucr/ucr-2019-1-nibrs-user-manual-093019.pdf",
    ),
    (
        "NIBRS Technical Specification",
        "https://le.fbi.gov/informational-tools/ucr/nibrs-technical-specification",
    ),
    ("A Word About UCR Data", "https://ucr.fbi.gov/a-word-about-ucr-data"),
    (
        "UCR Program Summary of Authorities",
        "https://ucr.fbi.gov/ucr-programsummary-of-authorities",
    ),
    (
        "Crime Data Explorer Documents and Downloads",
        "https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/downloads",
    ),
]

README = """# Auxiliary files — {dataset}.{table}

## Citation

{citation}

The data are a work of the United States Government and are therefore not
subject to copyright protection in the United States (17 U.S.C. § 105).

## Files in this bundle

{files}

## Documents kept at the publisher

These are large, stable and rarely opened, so they are linked rather than
copied here.

{links}

## What you need to know to read this table

{notes}

Bundle assembled on {today} from https://cde.ucr.cjis.gov/.
"""

TABLE_NOTES = {
    "agency": (
        "- `summary_months_reported` is the highest month the agency filed on the\n"
        "  Return A, not a count of complete months: an agency that filed only\n"
        "  October also records 10.\n"
        "- `covered_by_ori`, when populated, means this agency's crimes are\n"
        "  counted under the named agency. Summing both double counts.\n"
        "- `county_id` is matched from the published county name against the\n"
        "  Census ANSI county list and is null where the name has no unique match."
    ),
    "ucr_summary": (
        "- The offense codes are the 27 line items of the Return A form, not\n"
        "  NIBRS codes. Some are totals that add up the items below them, so\n"
        "  summing every item double counts.\n"
        "- Records of type 2 are adjustments and can carry negative counts that\n"
        "  correct a previously filed month.\n"
        "- `unfounded_count` is zero for every record before 1983."
    ),
    "hate_crime": (
        "- `offense_name`, `bias_description` and `victim_types` hold\n"
        "  semicolon-separated lists when the incident had more than one.\n"
        "- `offender_count` of 0 means the number of offenders is unknown, not\n"
        "  that there were none."
    ),
}
DEFAULT_NOTES = (
    "- NIBRS coverage is partial and grows over the period, from three states in\n"
    "  1991 to every state in 2020. A raw national sum is not comparable across\n"
    "  years. Use `agency.population` and `agency.nibrs_months_reported` to build\n"
    "  coverage weights.\n"
    "- Surrogate identifiers in the FBI's own download (`location_id`, `race_id`,\n"
    "  `weapon_id` and the rest) have been resolved to the FBI's published codes;\n"
    "  the code-to-label mapping is in the `dicionario` table.\n"
    "- Offenses must not be attributed to all of an incident's victims. Use the\n"
    "  `victim_offense` table for victim-level offense counts."
)


def fetch_sources():
    """Download the documents the bundles are built from."""
    INPUT.mkdir(parents=True, exist_ok=True)
    for name, key in SOURCES.items():
        target = INPUT / name
        if target.exists() and target.stat().st_size > 0:
            continue
        url = signed_url(key)
        if not url:
            raise RuntimeError(f"the CDE has no object at {key}")
        urllib.request.urlretrieve(url, target)
        print(f"fetched {name} ({target.stat().st_size:,} bytes)")

    methodology = INPUT / "hate_crime_methodology.pdf"
    if not methodology.exists():
        extracted = (
            INPUT / "extracted_hate_crime" / "Hate Crime Methodology.pdf"
        )
        if not extracted.exists():
            archive = INPUT / "hate_crime.zip"
            if not archive.exists():
                url = signed_url(constants.HATE_CRIME_KEY.value)
                urllib.request.urlretrieve(url, archive)
            with zipfile.ZipFile(archive) as zf:
                zf.extractall(INPUT / "extracted_hate_crime")
        shutil.copy(extracted, methodology)
        print(
            f"fetched hate_crime_methodology.pdf ({methodology.stat().st_size:,} bytes)"
        )


def build():
    BUNDLE_ROOT.mkdir(parents=True, exist_ok=True)
    built = {}
    for table, files in BUNDLES.items():
        directory = BUNDLE_ROOT / table
        directory.mkdir(parents=True, exist_ok=True)
        listed = []
        for name, note in files:
            source = INPUT / name
            shutil.copy(source, directory / name)
            key = SOURCES.get(name, constants.HATE_CRIME_KEY.value)
            listed.append(
                f"### `{name}`\n\n{note}\n\n"
                f"Downloaded {TODAY} from the Crime Data Explorer object "
                f"`{key}`, via its presigned-URL endpoint."
            )
        links = "\n".join(f"- [{title}]({url})" for title, url in LINK_ONLY)
        (directory / "README.md").write_text(
            README.format(
                dataset="us_fbi_cde",
                table=table,
                citation=CITATION,
                files="\n\n".join(listed),
                links=links,
                notes=TABLE_NOTES.get(table, DEFAULT_NOTES),
                today=TODAY,
            )
        )
        archive = BUNDLE_ROOT / f"{table}_auxiliary_files.zip"
        with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zf:
            for path in sorted(directory.iterdir()):
                zf.write(path, path.name)
        built[table] = archive
        print(f"{table:32s} {archive.stat().st_size:>10,} bytes")
    return built


def upload(built):
    for table, archive in built.items():
        destination = f"gs://{BUCKET}/auxiliary_files/us_fbi_cde/{table}/auxiliary_files.zip"
        subprocess.run(
            ["gcloud", "storage", "cp", str(archive), destination],
            check=True,
        )
        print(f"uploaded {destination}")


def verify(built):
    """Fetch every published URL anonymously and report what it actually returns."""
    print("\nanonymous HTTP status of each published URL")
    for table in built:
        url = (
            f"https://storage.googleapis.com/{BUCKET}/auxiliary_files/"
            f"us_fbi_cde/{table}/auxiliary_files.zip"
        )
        result = subprocess.run(
            ["curl", "-sI", url], capture_output=True, text=True
        )
        status = (
            result.stdout.splitlines()[0].strip()
            if result.stdout
            else "no response"
        )
        print(f"  {table:32s} {status}")
        print(f"    {url}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--upload", action="store_true")
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    fetch_sources()
    built = build()
    if args.upload:
        upload(built)
    if args.verify or args.upload:
        verify(built)


if __name__ == "__main__":
    main()
