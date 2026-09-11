"""Build the per-table auxiliary-file bundles for au_treasury_budget.

Each bundle holds the documents a user of *that* table needs in hand, plus a
README naming every file, where it came from and when it was downloaded.

What is deliberately not bundled
--------------------------------
The whole Budget Paper No. 1, Budget Paper No. 2 and the Intergenerational
Report PDFs. They are large, stable at the publisher and read once; the README
links them instead.

Upload
------
This script only *builds* the zips. Uploading them needs write access to the
auxiliary-file bucket, which the local dev service account does not have:

    ERROR: chave-subidores-de-dados@basedosdados-dev.iam.gserviceaccount.com
    does not have storage.objects.create access to ... basedosdados-public

Two separate things therefore block publishing these bundles, and both are
outside this dataset:

1. **No write permission** from a local machine, to either `basedosdados` or
   `basedosdados-public`.
2. **The documented bucket is requester-pays**, so every `auxiliaryFilesUrl`
   already published returns HTTP 400 `UserProjectMissing` to an anonymous
   visitor. Measured again on 2026-09-11:
   `basedosdados` -> 400, `basedosdados-public` -> 404 (nothing there yet).
   The move to `basedosdados-public` is committed on
   `fix/auxiliary-files-public-bucket` and not merged.

So ``Table.auxiliaryFilesUrl`` is left **unset** for this dataset rather than
pointing at a URL that does not resolve. Run this script and upload its output
once a credentialed path exists:

    gcloud storage cp output/auxiliary_files/<table>/auxiliary_files.zip \\
      gs://basedosdados-public/auxiliary_files/au_treasury_budget/<table>/
"""

from __future__ import annotations

import argparse
import datetime
import os
import pathlib
import zipfile

import releases

DATA_ROOT = pathlib.Path(
    os.environ.get(
        "AU_TREASURY_BUDGET_DATA",
        pathlib.Path.home() / "Downloads" / "au_treasury_budget_data",
    )
)
INPUT = DATA_ROOT / "input"

DOWNLOADED = datetime.date(2026, 9, 11)

CITATION = (
    "Commonwealth of Australia, the Treasury. Licensed CC BY 4.0. "
    "Attribution: (c) Commonwealth of Australia."
)

LINK_ONLY = {
    "aggregate": [
        (
            "Budget Paper No. 1, 2026-27 (complete, 6.99 MB PDF)",
            "https://budget.gov.au/content/bp1/download/bp1_2026-27.pdf",
        ),
        (
            "Budget archive, every release from 1970-71",
            "https://archive.budget.gov.au/",
        ),
    ],
    "payment_growth": [
        (
            "Budget Paper No. 1, Statement 3, 2026-27",
            "https://budget.gov.au/content/bp1/download/bp1_bs-3.pdf",
        ),
    ],
    "igr_projection": [
        (
            "2023 Intergenerational Report (complete PDF)",
            "https://treasury.gov.au/sites/default/files/2023-08/p2023-435150.pdf",
        ),
        (
            "2021 Intergenerational Report (PDF; its tables are not machine-readable)",
            "https://treasury.gov.au/sites/default/files/2021-06/p2021_182464.pdf",
        ),
    ],
}


def aggregate_members() -> list[tuple[pathlib.Path, str, str, str]]:
    """(source path, name in zip, what it is, where it came from)."""
    members = []
    for release in releases.releases_with_historical():
        assert release.historical_url
        members.append(
            (
                INPUT / "historical" / f"{release.release_id}.docx",
                f"historical_data/{release.release_id}.docx",
                f"Historical-data tables of the {release.label}, the document "
                f"every row of this table's {release.release_id} vintage is "
                "built from. Its footnotes define each measure",
                release.historical_url,
            )
        )
    return members


def payment_growth_members() -> list[tuple[pathlib.Path, str, str, str]]:
    members = []
    for release_id, url in (
        (
            "budget_2026_27",
            "https://budget.gov.au/content/bp1/download/bp1_bs-3.docx",
        ),
        (
            "budget_2025_26",
            "https://archive.budget.gov.au/2025-26/bp1/download/bp1_bs-3.docx",
        ),
        (
            "budget_2024_25",
            "https://archive.budget.gov.au/2024-25/bp1/download/bp1_bs-3.docx",
        ),
        (
            "budget_2022_23_october",
            "https://archive.budget.gov.au/2022-23-october/bp1/download/bp1_bs-3.docx",
        ),
    ):
        members.append(
            (
                INPUT / "prose" / f"{release_id}.docx",
                f"statement_3/{release_id}.docx",
                "Budget Paper No. 1, Statement 3. Its chart note states the "
                "projection window of each plotted series and that the growth "
                "rates are nominal -- neither is in the spreadsheet",
                url,
            )
        )
    for release in releases.releases_with_chart_data():
        assert release.chart_data_url
        members.append(
            (
                INPUT / "chart_data" / f"{release.release_id}.zip",
                f"chart_data/{release.release_id}.zip",
                f"Complete chart data published with the {release.label}. This "
                "table models one chart of it; the rest is here unmodelled",
                release.chart_data_url,
            )
        )
    return members


def igr_members() -> list[tuple[pathlib.Path, str, str, str]]:
    members = [
        (
            INPUT / "igr2023-chartdata.zip",
            "chart_data/igr2023_chart_data.zip",
            "Complete chart data of the 2023 Intergenerational Report: 130 "
            "sheets of annual series, far denser than the decadal appendix "
            "tables this table models. The sheets carry no chart titles, so a "
            "series cannot be given a unit without the report",
            releases.IGR_2023_CHART_ZIP,
        ),
    ]
    for name, description in (
        (
            "2023_IGR_A2_Key_concepts.docx",
            "Appendix A2, the report's definitions of every projected concept",
        ),
        (
            "2023_IGR_A3_Methodology_and_assumptions.docx",
            "Appendix A3, the projection methodology and the full assumption set",
        ),
        (
            "2023_IGR_A1_Projections_summary.docx",
            "Appendix A1, the projection tables this table's baseline is built from",
        ),
        (
            "2023_IGR_A4_Sensitivity_analysis.docx",
            "Appendix A4, the sensitivity analysis this table's six scenarios "
            "are built from",
        ),
    ):
        members.append(
            (
                INPUT / "igr2023w" / name,
                f"appendices/{name}",
                description,
                releases.IGR_2023_WORD_ZIP,
            )
        )
    return members


BUNDLES = {
    "aggregate": aggregate_members,
    "payment_growth": payment_growth_members,
    "igr_projection": igr_members,
}

TABLE_NOTES = {
    "aggregate": (
        "Every release in this bundle republishes the whole series back to "
        "1970-71 on its own basis. They disagree with each other about the same "
        "financial year, and that is not an error: each was correct as at its "
        "own date. The table keeps all twelve.\n\n"
        "The 2023-24 and 2020-21 Budgets are absent because those two releases "
        "published Budget Paper No. 1 as a single PDF with no per-statement "
        "DOCX; their financial years still appear through the neighbouring "
        "Final Budget Outcome and Budget vintages."
    ),
    "payment_growth": (
        'The growth rates are NOMINAL. The chart note reads "Shows major '
        "payments that are growing faster than nominal GDP over the projection "
        'period", and nothing in the chart is deflated.\n\n'
        "The projection window is per plotted series, not per chart, and lives "
        "in the statement prose rather than the spreadsheet -- which is why the "
        "Statement 3 documents are bundled here."
    ),
    "igr_projection": (
        "Only the 2023 edition is modelled. The 2021 edition published chart "
        "data but no Word bundle, so its projection and sensitivity tables exist "
        "only inside the PDF; the 2015 edition published neither. Neither was "
        "transcribed.\n\n"
        "The bundled chart data covers annual series the modelled table does "
        "not, but its sheets carry no chart titles in the 2023 edition, so a "
        "unit cannot be attached to a series without reading the report."
    ),
}


def build(table: str, out_root: pathlib.Path) -> pathlib.Path:
    members = BUNDLES[table]()
    missing = [path for path, *_ in members if not path.exists()]
    if missing:
        raise FileNotFoundError(
            f"{table}: {len(missing)} source file(s) missing, first {missing[0]}. "
            "Run download.py first."
        )

    lines = [
        f"# Auxiliary files -- au_treasury_budget.{table}",
        "",
        "## Citation",
        "",
        CITATION,
        "",
        "## Notes on this table",
        "",
        TABLE_NOTES[table],
        "",
        "## Bundled files",
        "",
    ]
    for _, name, description, url in members:
        lines += [
            f"### `{name}`",
            "",
            description,
            "",
            f"- Source: {url}",
            f"- Downloaded: {DOWNLOADED.isoformat()}",
            "",
        ]
    lines += [
        "## Linked, not bundled",
        "",
    ]
    for title, url in LINK_ONLY[table]:
        lines.append(f"- {title} -- {url}")
    lines += [
        "",
        "These are large, stable at the publisher and read once, so they are "
        "linked rather than rehosted.",
        "",
    ]

    directory = out_root / table
    directory.mkdir(parents=True, exist_ok=True)
    archive_path = directory / "auxiliary_files.zip"
    with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("README.md", "\n".join(lines))
        for path, name, _, _ in members:
            archive.write(path, name)
    size = archive_path.stat().st_size
    print(
        f"  {table:16s} {len(members):2d} files + README  {size / 1e6:5.1f} MB"
    )
    return archive_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out", default=str(DATA_ROOT / "output" / "auxiliary_files")
    )
    args = parser.parse_args()
    out_root = pathlib.Path(args.out)
    print("building auxiliary-file bundles")
    for table in BUNDLES:
        build(table, out_root)
    print(f"\nwritten to {out_root}")
    print(
        "NOT uploaded: the local service account has no storage.objects.create "
        "on basedosdados or basedosdados-public, and the documented bucket is "
        "requester-pays so its links return HTTP 400 anyway. "
        "auxiliary_files_url is therefore left unset."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
