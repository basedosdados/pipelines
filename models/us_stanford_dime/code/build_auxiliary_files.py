"""Build and upload the per-table auxiliary-file bundles.

DIME ships two documents that a user of these tables needs in hand: the version
4.0 codebook, which is the only place the FEC transaction codes, seat labels and
variable definitions are written down, and the validation compendium behind the
CFscores. Both are small — 333 KB and 140 KB — so both are bundled rather than
linked; the link-only rule is for long-form PDFs in the tens of megabytes.

Two caveats are reported rather than hidden:

* The documented location is the **prod** bucket, but the credentials available
  here can only write to ``basedosdados-dev`` (both ``basedosdados`` and
  ``basedosdados-public`` return 403 on a metadata read). The bundles go to the
  dev bucket, which is where most existing rows point anyway, and the URL needs
  re-pointing by someone with prod credentials before the dataset is published.
* Both buckets are requester-pays, so an anonymous fetch of any
  ``auxiliaryFilesUrl`` returns HTTP 400 ``UserProjectMissing``. That is true of
  all 84 production tables using the field today; it is a bucket setting, not
  something to solve per dataset. ``--verify`` fetches each URL with no
  credentials and prints what actually comes back.

    export GOOGLE_APPLICATION_CREDENTIALS=<dev service account key>
    uv run --with google-cloud-storage python build_auxiliary_files.py --verify
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import clean
import upload

BUCKET = "basedosdados-dev"
DATASET = "us_stanford_dime"
DOWNLOADED = "2026-09-02"

DOCS = {
    "dime_codebook_v4.pdf": (
        "https://bit.ly/dimeV4codebook",
        "DIME v4.0 codebook (Bonica, 29 December 2024). Defines every variable, "
        "and is the only published source for the FEC transaction type codes "
        "(section 8) and the seat labels (section 7).",
    ),
    "dime_validation_results.pdf": (
        "https://www.dropbox.com/s/2ntrx9auzifgujp/dime_validation.pdf?dl=1",
        "Compendium of validation results for the CFscore ideology measures, "
        "across several studies and institutional settings.",
    ),
}

# Tables that get a bundle, and what a user of each needs to know.
TABLE_NOTES = {
    "contribution": (
        "Section 5.3 of the codebook documents this table's variables, section 8 "
        "lists the transaction codes and section 7 the seat labels. Note the "
        "codebook describes three columns (contributor.district.90s / .00s / "
        ".10s) that the published files do not contain; the files carry a single "
        "contributor.district, which is what this table exposes."
    ),
    "recipient": (
        "Section 5.1 of the codebook documents this table's variables. It is "
        "built from dime_recipients_all, the superset that also contains "
        "recipients excluded from the CFscore scaling; included_in_scaling "
        "recovers the smaller dime_recipients file."
    ),
    "contributor": (
        "Section 5.2 of the codebook documents this table's variables. The "
        "codebook lists a num.records column and names the coordinate columns "
        "most.recent.latitude / .longitude; the published file has neither — it "
        "carries num.distinct only, and names the coordinates "
        "most.recent.contributor.latitude / .longitude."
    ),
    "contributor_cycle": (
        "Reshaped from the amount.<cycle> columns described at the end of "
        "section 5.2 of the codebook. Only non-zero donor-cycle pairs are kept."
    ),
}

CITATION = (
    "Bonica, Adam. 2024. Database on Ideology, Money in Politics, and Elections: "
    "Public version 4.0 [Computer file]. Stanford, CA: Stanford University "
    "Libraries. https://data.stanford.edu/dime\n\n"
    "For the CFscore measures, cite additionally:\n"
    'Bonica, Adam. 2014. "Mapping the Ideological Marketplace." American '
    "Journal of Political Science 58 (2): 367-387.\n\n"
    "For the DW-DIME measures, cite additionally:\n"
    'Bonica, Adam. 2018. "Inferring Roll-Call Scores from Campaign '
    'Contributions Using Supervised Machine Learning." American Journal of '
    "Political Science 62 (4): 830-848."
)


def readme(table: str) -> str:
    files = "\n".join(
        f"- `{name}` — {desc}\n  Downloaded {DOWNLOADED} from {url}"
        for name, (url, desc) in DOCS.items()
    )
    return f"""# Auxiliary files — us_stanford_dime.{table}

Documentation published alongside the Database on Ideology, Money in Politics,
and Elections (DIME), version 4.0.

## How to cite

{CITATION}

## Licence

ODC-BY 1.0. Share, create and adapt with attribution.
Full text: https://opendatacommons.org/licenses/by/1-0/

## Files in this bundle

{files}

## About this table

{TABLE_NOTES[table]}

## What Data Basis changed

The transform is thin. Source columns were renamed to the Data Basis
architecture names, the source's `\\N` missing marker was converted to a real
NULL, and each column was cast to its declared type. No rows were filtered, no
values were recoded, and the dictionary-covered columns keep the source's own
codes rather than being decoded in place — `us_stanford_dime.dicionario` maps
them to labels.

Two source quirks were repaired rather than passed through:

- A small number of lines carry bytes that are not valid UTF-8 (one in the 1990
  cycle, one in 2006). Those byte sequences were dropped with `iconv -c`; the
  affected rows are kept.
- The codebook's per-file row counts are counts of physical lines, not records.
  They exceed the record count wherever a field contains a newline inside
  quotes, so they are an upper bound on what you should expect to find here.

## Not included

The CRP/NIMSP itemized contribution records (27,352,201 rows) are excluded from
the public DIME release and are licensed CC BY-NC-SA rather than ODC-BY. They
are available from bonica@stanford.edu for academic use only.
"""


def build(out_dir: Path) -> dict[str, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    src = {
        name: Path(f"/tmp/{local}")
        for name, local in {
            "dime_codebook_v4.pdf": "dime_codebook.pdf",
            "dime_validation_results.pdf": "dime_validation.pdf",
        }.items()
    }
    missing = [str(p) for p in src.values() if not p.exists()]
    if missing:
        raise FileNotFoundError(f"source documents not present: {missing}")

    bundles = {}
    for table in TABLE_NOTES:
        dest = out_dir / f"{table}_auxiliary_files.zip"
        with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr("README.md", readme(table))
            for name, path in src.items():
                z.write(path, name)
        bundles[table] = dest
        print(f"built {dest.name} ({dest.stat().st_size / 1024:.0f} KB)")
    return bundles


def blob_name(table: str) -> str:
    return f"auxiliary_files/{DATASET}/{table}/auxiliary_files.zip"


def public_url(table: str) -> str:
    return f"https://storage.googleapis.com/{BUCKET}/{blob_name(table)}"


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--upload", action="store_true")
    p.add_argument(
        "--verify", action="store_true", help="fetch each URL anonymously"
    )
    args = p.parse_args()

    out_dir = clean.SCRATCH / "auxiliary_files"
    bundles = build(out_dir)

    if args.upload:
        client = upload._storage()
        for table, path in bundles.items():
            upload.upload_blob(path, blob_name(table), client)
            print(f"uploaded {table} -> gs://{BUCKET}/{blob_name(table)}")

    if args.verify:
        print("\nAnonymous fetch of each published URL:")
        for table in bundles:
            url = public_url(table)
            out = subprocess.run(
                ["curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}", url],
                capture_output=True,
                text=True,
            )
            code = out.stdout.strip()
            verdict = "OK" if code == "200" else "NOT PUBLICLY READABLE"
            print(f"  {table:<20} HTTP {code}  {verdict}")
        print(
            "\nHTTP 400 is expected while the bucket is requester-pays; it "
            "affects every table on the site that uses this field."
        )


if __name__ == "__main__":
    main()
