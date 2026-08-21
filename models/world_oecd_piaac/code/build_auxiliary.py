"""Build the per-table auxiliary-file bundles for world_oecd_piaac.

Usage:
    uv run python models/world_oecd_piaac/code/build_auxiliary.py [--upload] [--env dev|prod]

Follows .claude/rules/auxiliary-files.md: one ZIP per table containing only the
documents a user of that table needs, each bundle carrying a README with the
citation, per-file provenance and download dates, and an index of the documents
that are linked rather than rehosted.
"""

from __future__ import annotations

import datetime as dt
import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import constants as piaac

BUNDLE_ROOT = piaac.OUTPUT_ROOT / "auxiliary_files"
DOWNLOADED_ON = dt.date(2026, 8, 21).isoformat()

# Documents www.oecd.org serves only to a real browser session. Every scripted
# client gets 403, with or without browser headers, cookies or the CDN rendition
# path, so these are indexed with their URLs rather than rehosted.
BROWSER_ONLY = {
    "1": [
        (
            "International Master Background Questionnaire (Cycle 1)",
            f"{piaac.DAM}/background-questionnaire/cycle-1/Master-BQ.zip",
        ),
        (
            "Stata do-file to import the CSV Public Use Files (Cycle 1)",
            f"{piaac.DAM}/data-materials/import%20PUFs%20in%20stata.zip",
        ),
    ],
    "2": [
        (
            "International Master Background Questionnaire (Cycle 2)",
            f"{piaac.DAM}/background-questionnaire/cycle-2/master-bq-cy2.zip",
        ),
        (
            "Flowcharts of all Background Questionnaire sections (Cycle 2)",
            f"{piaac.DAM}/background-questionnaire/cycle-2/piaac-cy2-round-1-flowcharts-of-background-questionnaire.zip",
        ),
        (
            "Stata do-file to import the CSV Public Use Files (Cycle 2)",
            f"{piaac.DAM}/data-materials/cycle-2/import-csv-in-stata.zip",
        ),
        (
            "R script to import the CSV Public Use Files (Cycle 2)",
            f"{piaac.DAM}/data-materials/cycle-2/import-csv-in-r.zip",
        ),
    ],
}

WHAT_IT_IS = {
    "international_codebook.xlsx": "Every variable in the Public Use Files with its label, measurement level, "
    "value scheme and missing scheme. The schema source of truth for this dataset",
    "derived_variables_codebook.docx": "How each derived variable is constructed from the questionnaire responses",
    "derived_variables_codebook_and_sql.pdf": "How each derived variable is constructed, with the SQL used to build it",
    "missing_variables_by_country.xls": "Variables coded as missing in a given country's file because national data "
    "protection law does not permit their release",
    "missing_variables_by_country.xlsx": "Variables coded as missing in a given country's file because national data "
    "protection law does not permit their release",
    "background_questionnaire_framework.pdf": "Conceptual framework behind the background questionnaire",
    "compendium_background.xlsx": "Categorical percentages for every background variable by country. Use it to "
    "check that your own analysis reproduces the published figures",
    "compendium_cognitive.xlsx": "Categorical percentages for every cognitive variable by country",
}
for _round in ("1", "2", "3"):
    WHAT_IT_IS[f"compendium_background_round_{_round}.xlsx"] = (
        f"Categorical percentages for every background variable by country, Round {_round}. "
        "Use it to check that your own analysis reproduces the published figures"
    )
    WHAT_IT_IS[f"compendium_cognitive_round_{_round}.xlsx"] = (
        f"Categorical percentages for every cognitive variable by country, Round {_round}"
    )

# Which documents belong with which table.
BUNDLES: dict[str, tuple[list[str], list[str]]] = {
    # table slug: (cycles whose documents to include, filename filters; [] = all)
    "respondent_cycle_1": (["1"], []),
    "respondent_cycle_2": (["2"], []),
    "respondent_cycle_1_usa_national": (
        ["1"],
        ["international_codebook", "missing_variables"],
    ),
    "item_response_cycle_1": (
        ["1"],
        [
            "international_codebook",
            "compendium_cognitive",
            "background_questionnaire_framework",
        ],
    ),
    "item_response_cycle_2": (
        ["2"],
        [
            "international_codebook",
            "compendium_cognitive",
            "background_questionnaire_framework",
        ],
    ),
    "variable": (["1", "2"], ["international_codebook"]),
    "dictionary": (["1", "2"], ["international_codebook"]),
}

TABLE_NOTES = {
    "respondent_cycle_1_usa_national": (
        "This table holds the United States Round 3 (2017) *national* Public Use "
        "File. The OECD publishes no internationally comparable file for that round "
        "in the United States. It adds 131 US-specific variables that the "
        "international codebook does not describe, and suppresses 110 international "
        "ones including exact age, which is why it is separate from "
        "respondent_cycle_1 rather than stacked into it."
    ),
    "item_response_cycle_1": (
        "One row per respondent and item. Respondents see only a subset of the item "
        "pool, so a row exists only where the assessment recorded at least one "
        "measure."
    ),
    "item_response_cycle_2": (
        "One row per respondent and item. Respondents see only a subset of the item "
        "pool, so a row exists only where the assessment recorded at least one "
        "measure."
    ),
}

LOADING_NOTES = """## What was changed when loading

- **Reserved codes were removed from numeric columns.** PIAAC pads them to the
  width of the field, so hourly earnings carry `999999999996` for "valid skip".
  Leaving them in would put a trillion-dollar wage into every average. Each
  column's description in the Data Basis metadata lists exactly which values were
  set to NULL. Categorical columns keep their reserved codes, because "refused"
  and "don't know" are answers; look them up in the `dictionary` table.
- **The CSV files use the SAS special-missing codes**, not the SPSS numeric codes
  the codebook lists beside them. Cycle 1 writes them bare and upper-cased (`N`),
  Cycle 2 dotted and lower-cased (`.n`). A bare `.` means "not administered" and
  is loaded as NULL.
- **Item responses are stored long**, one row per respondent and item, rather than
  as the ~1,700 columns of the source file. `scored_response_label` carries the
  decoded meaning, because PIAAC's scoring codes are item-specific: code 1 is
  "Full Credit" for most items but "Partial Credit" for others.
- **`year`, `cycle` and `round` are derived**, since PIAAC carries no
  year-of-collection variable. Cycle 1 Round 1 is 2012, Round 2 is 2015, Round 3
  is 2017; Cycle 2 Round 1 is 2023.
"""


def build_readme(table_slug: str, files: list[Path], cycles: list[str]) -> str:
    lines = [
        f"# Auxiliary files for `world_oecd_piaac.{table_slug}`",
        "",
        "## Citation",
        "",
        piaac.CITATION,
        "",
    ]
    if table_slug in TABLE_NOTES:
        lines += ["## About this table", "", TABLE_NOTES[table_slug], ""]
    lines += ["## Files in this bundle", ""]
    for path in files:
        cycle = path.parent.name.replace("cycle_", "")
        url = dict(piaac.DOCS[cycle]).get(path.name, "")
        lines += [
            f"### `{path.name}`",
            "",
            WHAT_IT_IS.get(
                path.name, "Supporting documentation published by the OECD."
            ),
            "",
            f"- Source: {url}",
            f"- Downloaded: {DOWNLOADED_ON}",
            f"- Cycle: {cycle}",
            "",
        ]
    lines += [
        "## Linked, not included",
        "",
        "Long-form methodology reports are stable at the OECD and run to tens of "
        "megabytes, so they are linked rather than copied here.",
        "",
    ]
    for title, url in piaac.REFERENCE_DOCUMENTS:
        lines.append(f"- [{title}]({url})")
    lines += [
        "",
        "The following are also linked rather than included, for a different reason: "
        "www.oecd.org returns HTTP 403 for every `.zip` to any scripted client, with "
        "or without browser headers, cookies or the CDN rendition path. They download "
        "normally in a browser.",
        "",
    ]
    for cycle in cycles:
        for title, url in BROWSER_ONLY.get(cycle, []):
            lines.append(f"- [{title}]({url})")
    lines += ["", LOADING_NOTES]
    return "\n".join(lines)


def build_bundle(table_slug: str) -> Path:
    cycles, filters = BUNDLES[table_slug]
    files: list[Path] = []
    for cycle in cycles:
        folder = piaac.DOCS_ROOT / f"cycle_{cycle}"
        for path in sorted(folder.glob("*")):
            if path.is_dir():
                continue
            if filters and not any(f in path.name for f in filters):
                continue
            files.append(path)

    BUNDLE_ROOT.mkdir(parents=True, exist_ok=True)
    destination = BUNDLE_ROOT / table_slug
    destination.mkdir(parents=True, exist_ok=True)
    archive = destination / "auxiliary_files.zip"
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("README.md", build_readme(table_slug, files, cycles))
        for path in files:
            prefix = (
                f"cycle_{path.parent.name.replace('cycle_', '')}_"
                if len(cycles) > 1
                else ""
            )
            zf.write(path, f"{prefix}{path.name}")
    return archive


def main() -> None:
    total = 0
    for table_slug in BUNDLES:
        archive = build_bundle(table_slug)
        size = archive.stat().st_size
        total += size
        with zipfile.ZipFile(archive) as zf:
            n = len(zf.namelist())
        print(f"  {table_slug:<34} {n:>2} files  {size / 1e6:>6.1f} MB")
    print(f"  {'total':<34}    {total / 1e6:>9.1f} MB")


if __name__ == "__main__":
    main()
