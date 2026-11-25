"""Release manifest for au_treasury_budget.

Every fiscal release the Australian Treasury publishes is its own *vintage*: the
numbers in the 2022-23 October Budget and the 2023-24 Budget are both correct as
at their own date, and neither supersedes the other. This module is the single
declaration of which releases exist, where their machine-readable documents live,
and what each release is called.

Why the URLs are enumerated rather than derived
-----------------------------------------------
Nothing about budget.gov.au is stable across releases:

* the historical-data statement moves between **Statement 10 and Statement 11**
  (2025-26 and 2022-23 March use 10; every other year uses 11);
* the per-statement file is ``bp1_bs-11.docx`` in some years and ``bp1_bs11.docx``
  in others;
* the chart-data archive is variously ``chartdata.zip``,
  ``chart-data-final.zip`` and ``budget_2022-23_chart_data.zip``;
* the live Budget sits on ``budget.gov.au/content/`` while every prior one sits on
  ``archive.budget.gov.au/<year>/`` with the ``content/`` segment dropped.

A derived URL would therefore be wrong for most releases, and — worse — would be
*silently* wrong, because budget.gov.au answers an unknown path with HTTP 200 and
a 7,349-byte "page not found" HTML body rather than a 404. Every download here is
size- and magic-checked for that reason.

Excluded, deliberately
----------------------
* **2023-24 Budget and 2020-21 Budget** — BP1 ships as a single PDF. No
  per-statement DOCX exists at any of the 10 filename patterns tried. Their
  financial years still appear through the neighbouring FBO and Budget vintages.
* **MYEFO** contributes no historical aggregates. Its only spreadsheet, Appendix C
  Annex A, is payments to the states. MYEFO appears here only for chart data.
* **2015 IGR** — published as PDF only, with no chart data and no Word bundle.
"""

from __future__ import annotations

from dataclasses import dataclass

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
)

#: budget.gov.au serves this many bytes of "page not found" HTML with a 200 status.
SOFT_404_BYTES = 7349


@dataclass(frozen=True)
class Release:
    """One fiscal release, and the documents it published."""

    release_id: str
    label: str
    release_type: str  # budget | final_budget_outcome
    #: Financial year the release is *named* for, as "YYYY-YY".
    release_financial_year: str
    #: DOCX carrying the historical-data tables, or None when only a PDF exists.
    historical_url: str | None = None
    #: Table-number prefix used inside that DOCX: "11", "10" or "B".
    historical_prefix: str = "11"
    #: Chart-data archive, when the release published one.
    chart_data_url: str | None = None
    #: BP1 statement DOCX carrying the payment-growth chart's prose.
    statement_prose_url: str | None = None
    notes: str = ""


ARCHIVE = "https://archive.budget.gov.au"
LIVE = "https://budget.gov.au/content"


RELEASES: tuple[Release, ...] = (
    Release(
        release_id="budget_2026_27",
        label="2026-27 Budget",
        release_type="budget",
        release_financial_year="2026-27",
        historical_url=f"{LIVE}/bp1/download/bp1_bs-11.docx",
        historical_prefix="11",
        chart_data_url=f"{LIVE}/download/chart-data-final.zip",
        statement_prose_url=f"{LIVE}/bp1/download/bp1_bs-3.docx",
    ),
    Release(
        release_id="budget_2025_26",
        label="2025-26 Budget",
        release_type="budget",
        release_financial_year="2025-26",
        historical_url=f"{ARCHIVE}/2025-26/bp1/download/bp1_bs-10.docx",
        historical_prefix="10",
        chart_data_url=f"{ARCHIVE}/2025-26/download/chart-data-final.zip",
        statement_prose_url=f"{ARCHIVE}/2025-26/bp1/download/bp1_bs-3.docx",
        notes="Historical data is Statement 10, not 11.",
    ),
    Release(
        release_id="budget_2024_25",
        label="2024-25 Budget",
        release_type="budget",
        release_financial_year="2024-25",
        historical_url=f"{ARCHIVE}/2024-25/bp1/download/bp1_bs-11.docx",
        historical_prefix="11",
        chart_data_url=f"{ARCHIVE}/2024-25/download/chartdata.zip",
        statement_prose_url=f"{ARCHIVE}/2024-25/bp1/download/bp1_bs-3.docx",
    ),
    Release(
        release_id="budget_2023_24",
        label="2023-24 Budget",
        release_type="budget",
        release_financial_year="2023-24",
        historical_url=None,
        chart_data_url=f"{ARCHIVE}/2023-24/download/chartdata.zip",
        notes="BP1 published as a single PDF; no per-statement DOCX exists. "
        "Chart data only.",
    ),
    Release(
        release_id="budget_2022_23_october",
        label="2022-23 October Budget",
        release_type="budget",
        release_financial_year="2022-23",
        historical_url=f"{ARCHIVE}/2022-23-october/bp1/download/bp1_bs-11.docx",
        historical_prefix="11",
        chart_data_url=f"{ARCHIVE}/2022-23-october/download/chartdata.zip",
        notes="Second Budget of 2022-23, delivered after the May election.",
    ),
    Release(
        release_id="budget_2022_23_march",
        label="2022-23 March Budget",
        release_type="budget",
        release_financial_year="2022-23",
        historical_url=f"{ARCHIVE}/2022-23/bp1/download/bp1_bs10.docx",
        historical_prefix="10",
        chart_data_url=f"{ARCHIVE}/2022-23/download/budget_2022-23_chartdata.zip",
        notes="Historical data is Statement 10. Filename drops the hyphen.",
    ),
    Release(
        release_id="budget_2021_22",
        label="2021-22 Budget",
        release_type="budget",
        release_financial_year="2021-22",
        historical_url=f"{ARCHIVE}/2021-22/bp1/download/bp1_bs11.docx",
        historical_prefix="11",
        chart_data_url=f"{ARCHIVE}/2021-22/download/budget_2021-22_chart_data.zip",
    ),
    Release(
        release_id="budget_2020_21",
        label="2020-21 Budget",
        release_type="budget",
        release_financial_year="2020-21",
        historical_url=None,
        chart_data_url=f"{ARCHIVE}/2020-21/download/budget_2020-21_chart_data.zip",
        notes="BP1 published as a single PDF; no per-statement DOCX exists. "
        "Chart data only.",
    ),
    Release(
        release_id="fbo_2024_25",
        label="2024-25 Final Budget Outcome",
        release_type="final_budget_outcome",
        release_financial_year="2024-25",
        historical_url=f"{ARCHIVE}/2024-25/fbo/download/06_appendix_b.docx",
        historical_prefix="B",
    ),
    Release(
        release_id="fbo_2023_24",
        label="2023-24 Final Budget Outcome",
        release_type="final_budget_outcome",
        release_financial_year="2023-24",
        historical_url=f"{ARCHIVE}/2023-24/fbo/download/06_appendix_b.docx",
        historical_prefix="B",
    ),
    Release(
        release_id="fbo_2022_23",
        label="2022-23 Final Budget Outcome",
        release_type="final_budget_outcome",
        release_financial_year="2022-23",
        historical_url=f"{ARCHIVE}/2022-23-october/fbo/download/06_appendix_b.docx",
        historical_prefix="B",
        notes="The 2022-23 FBO is filed under the 2022-23-october archive path, "
        "not under 2022-23.",
    ),
    Release(
        release_id="fbo_2021_22",
        label="2021-22 Final Budget Outcome",
        release_type="final_budget_outcome",
        release_financial_year="2021-22",
        historical_url=f"{ARCHIVE}/2021-22/fbo/download/06_appendix_b.docx",
        historical_prefix="B",
    ),
    Release(
        release_id="fbo_2020_21",
        label="2020-21 Final Budget Outcome",
        release_type="final_budget_outcome",
        release_financial_year="2020-21",
        historical_url=f"{ARCHIVE}/2020-21/fbo/download/06_appendix_b.docx",
        historical_prefix="B",
    ),
    Release(
        release_id="fbo_2019_20",
        label="2019-20 Final Budget Outcome",
        release_type="final_budget_outcome",
        release_financial_year="2019-20",
        historical_url=f"{ARCHIVE}/2019-20/fbo/download/06_appendix_b.docx",
        historical_prefix="B",
        notes="Table captions are written 'Table B1', without the dot that every "
        "later edition uses.",
    ),
    Release(
        release_id="myefo_2025_26",
        label="2025-26 MYEFO",
        release_type="myefo",
        release_financial_year="2025-26",
        chart_data_url=f"{ARCHIVE}/2025-26/myefo/download/myefo-2025-26-chart-data.zip",
    ),
    Release(
        release_id="myefo_2024_25",
        label="2024-25 MYEFO",
        release_type="myefo",
        release_financial_year="2024-25",
        chart_data_url=f"{ARCHIVE}/2024-25/myefo/download/myefo-2024-25-chart-data.zip",
    ),
    Release(
        release_id="myefo_2023_24",
        label="2023-24 MYEFO",
        release_type="myefo",
        release_financial_year="2023-24",
        chart_data_url=f"{ARCHIVE}/2023-24/myefo/download/myefo-2023-24-chart-data.zip",
    ),
)


#: The Intergenerational Report. 2023 is the most recent edition and the only one
#: whose projection tables and sensitivity analysis are machine-readable: the 2021
#: edition published chart data but no Word bundle, and the 2015 edition published
#: neither.
IGR_2023_WORD_ZIP = (
    "https://treasury.gov.au/sites/default/files/2023-08/p2023-435150-word.zip"
)
IGR_2023_CHART_ZIP = (
    "https://treasury.gov.au/sites/default/files/2023-09/p2023-435150-cd.zip"
)
IGR_EDITION = "2023"


def releases_with_historical() -> list[Release]:
    """Releases that republish the historical-data tables."""
    return [r for r in RELEASES if r.historical_url]


def releases_with_chart_data() -> list[Release]:
    """Releases that published a chart-data archive."""
    return [r for r in RELEASES if r.chart_data_url]


def by_id(release_id: str) -> Release:
    for r in RELEASES:
        if r.release_id == release_id:
            return r
    raise KeyError(release_id)


def financial_year_start(financial_year: str) -> int:
    """``"1970-71"`` -> ``1970``."""
    return int(financial_year[:4])
