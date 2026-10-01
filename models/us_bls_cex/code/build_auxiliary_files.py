"""Build the per-table auxiliary-file bundles for us_bls_cex.

Writes ``<DATA_DIR>/auxiliary_files/<table>/auxiliary_files.zip``, each with a
README.md, for upload to
``gs://basedosdados-public/auxiliary_files/us_bls_cex/<table>/``
(see .claude/rules/auxiliary-files.md). The LABSTAT tables get no bundle: their
only documentation is an HTML guide, linked from the dataset instead.
"""

import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_bls_cex.pumd_files import DATA_DIR, DOCS_DIR

OUT = DATA_DIR / "auxiliary_files"
DOWNLOADED = "2026-10-01"
BASE = "https://www.bls.gov/cex"

# bundle name -> (source file in docs/, source URL, what it is)
DOCS = {
    "pumd_dictionary.xlsx": (
        "ce-pumd-interview-diary-dictionary.xlsx",
        f"{BASE}/pumd/ce-pumd-interview-diary-dictionary.xlsx",
        "BLS Interview and Diary PUMD dictionary: every variable with its "
        "description, formula, flag variable, first and last year (Variables "
        "sheet) and every code value with its label (Codes sheet)",
    ),
    "hierarchical_groupings.zip": (
        "stubs.zip",
        f"{BASE}/pumd/stubs.zip",
        "BLS hierarchical grouping (stub) files, 1996-2024: the UCC category "
        "tree for the integrated, interview and diary publications, as loaded "
        "into the ucc table",
    ),
    "ucc_source_selection.xlsx": (
        "ce_source_integrate.xlsx",
        f"{BASE}/ce_source_integrate.xlsx",
        "Source selection file, 1996-2024: which survey (Interview or Diary) "
        "BLS uses to estimate each UCC in the integrated tables",
    ),
    "sample_code_r_ucc.zip": (
        "r-ucc.zip",
        f"{BASE}/pumd/r-ucc.zip",
        "BLS sample R program: calendar-year mean and standard error for a UCC",
    ),
    "sample_code_stata_ucc.zip": (
        "stata-ucc.zip",
        f"{BASE}/pumd/stata-ucc.zip",
        "BLS sample Stata programs: calendar-year mean and standard error for a UCC",
    ),
    "sample_code_sas_ucc.zip": (
        "sas-ucc.zip",
        f"{BASE}/pumd/sas-ucc.zip",
        "BLS sample SAS program: calendar-year mean and standard error for a UCC",
    ),
    "sample_code_sas_tables.zip": (
        "sas-table.zip",
        f"{BASE}/pumd/sas-table.zip",
        "BLS sample SAS programs reproducing the published Interview, Diary and "
        "integrated tables",
    ),
    "sample_code_sas_macros.zip": (
        "sas-macros.zip",
        f"{BASE}/pumd/sas-macros.zip",
        "BLS SAS macros for PUMD estimation, with documentation",
    ),
}

SAMPLE_CODE = [k for k in DOCS if k.startswith("sample_code")]
BUNDLES = {
    "interview_household": ["pumd_dictionary.xlsx", *SAMPLE_CODE],
    "interview_member": ["pumd_dictionary.xlsx"],
    "interview_expenditure": [
        "pumd_dictionary.xlsx",
        "hierarchical_groupings.zip",
        "ucc_source_selection.xlsx",
        *SAMPLE_CODE,
    ],
    "interview_income": ["pumd_dictionary.xlsx", "hierarchical_groupings.zip"],
    "diary_household": ["pumd_dictionary.xlsx", *SAMPLE_CODE],
    "diary_member": ["pumd_dictionary.xlsx"],
    "diary_expenditure": [
        "pumd_dictionary.xlsx",
        "hierarchical_groupings.zip",
        "ucc_source_selection.xlsx",
        *SAMPLE_CODE,
    ],
    "diary_income": ["pumd_dictionary.xlsx", "hierarchical_groupings.zip"],
    "ucc": ["hierarchical_groupings.zip", "ucc_source_selection.xlsx"],
}

LINKS = [
    ("PUMD Getting Started Guide", f"{BASE}/pumd-getting-started-guide.htm"),
    (
        "PUMD documentation index (errata, survey forms)",
        f"{BASE}/pumd_doc.htm",
    ),
    ("Disclosure and topcoding", f"{BASE}/pumd_disclosure.htm"),
    ("Income imputation user's guide (PDF)", f"{BASE}/csxguide.pdf"),
    (
        "Handbook of Methods: Consumer Expenditures and Income",
        "https://www.bls.gov/opub/hom/cex/",
    ),
    (
        "LABSTAT Getting Started Guide",
        f"{BASE}/labstat/ce-labstat-getting-started-guide.htm",
    ),
    (
        "Impact of the 2025 federal shutdown on CE data",
        f"{BASE}/2025-federal-government-shutdown-impact-ce.htm",
    ),
]

NOTES = """\
## How Data Basis loaded these tables

- **Years.** Microdata from the 1996 through 2024 BLS releases. Interview tables
  run from 1996 Q1 to 2025 Q1, because the 2024 release carries 2025 Q1; diary
  tables run 1996-2024. `year` and `quarter` are the collection quarter, taken
  from the BLS file name (e.g. `fmli242` = 2024 Q2), not the release year.
- **Overlapping quarters.** Before 2020 each interview release repeats the first
  quarter of the following year, which the next release ships again as an `x`
  file reprocessed under newer rules. Each quarter is loaded once, from the
  release of its own year (the `x` file). BLS's own calendar-year programs use
  the five quarters of a single release instead, so a reproduction of a
  published table should read Q1 of year Y+1 knowing it may differ slightly.
- **Names.** Survey variables keep their BLS names, lowercased. Keys, dates and
  weights are renamed: `consumer_unit_id` and `interview_number` / `diary_week`
  are NEWID split at its last digit; `final_weight` is FINLWT21;
  `replicate_weight_01`-`44` are WTREP01-44; `member_number` is MEMBNO;
  `reference_year` / `reference_month` are REF_YR / REF_MO.
- **Interview numbering.** Interviews are numbered 2-5 through 2015, when the
  first (bounding) interview was collected but not released, and 1-4 after BLS
  dropped it.
- **Missing values.** A blank field or a bare `.` (used through 2016) is NULL.
  Flag columns (BLS name plus `_`) say why: A valid blank, B invalid blank,
  C don't know or refused, D valid, E allocated, F imputed, G allocated and
  imputed, H parent record, T topcoded or suppressed, U/V/W topcoded after
  allocation or imputation. Labels are in the `dicionario` table.
- **Codes.** All-digit codes are stored without leading zeros (`01` -> `1`), so
  a code reads the same in every year; BLS changed the padding of several
  variables over time. UCCs keep their six digits. NEWID likewise loses its
  leading zeros.
- **Types.** Dollar amounts, counts and ages are numeric; coded variables and
  identifiers are text. Sixteen variables absent from the BLS dictionary are
  stored as text.
"""

CITATION = (
    "U.S. Bureau of Labor Statistics, Consumer Expenditure Surveys, Interview and "
    "Diary Public Use Microdata, 1996-2024 releases."
)


def readme(table: str, files: list[str]) -> str:
    lines = [
        f"# us_bls_cex.{table}: auxiliary files",
        "",
        "Source: U.S. Bureau of Labor Statistics, Consumer Expenditure Surveys (CE).",
        "BLS publications are in the public domain.",
        "",
        "## Citation",
        "",
        CITATION,
        "",
        "## Files in this bundle",
        "",
        "| File | What it is | Source | Downloaded |",
        "|---|---|---|---|",
    ]
    for f in files:
        _, url, what = DOCS[f]
        lines.append(f"| `{f}` | {what} | {url} | {DOWNLOADED} |")
    lines += ["", "## Reference documents (not bundled)", ""]
    lines += [f"- [{title}]({url})" for title, url in LINKS]
    lines += ["", NOTES]
    return "\n".join(lines)


def main():
    for table, files in BUNDLES.items():
        d = OUT / table
        d.mkdir(parents=True, exist_ok=True)
        zp = d / "auxiliary_files.zip"
        with zipfile.ZipFile(zp, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr("README.md", readme(table, files))
            for f in files:
                z.write(DOCS_DIR / DOCS[f][0], f)
        print(f"{table}: {zp.stat().st_size / 1e6:.1f} MB, {len(files)} files")


if __name__ == "__main__":
    main()
