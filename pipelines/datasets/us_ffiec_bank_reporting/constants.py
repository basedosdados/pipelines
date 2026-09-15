"""Constants for the us_ffiec_bank_reporting recurring pipeline.

The dataset splits across two cadences, so it has two flows:

* the Call Report and the FR Y-9C are **quarterly**, filed roughly 30-45 days
  after the quarter ends and then *amended* for several quarters afterwards;
* the CRA disclosure is **annual**, released around the end of the following
  year.

Amendments are why the quarterly flow re-materialises a trailing window rather
than appending. A restated 2026Q1 filing changes values already published, so
the only correct refresh is to rebuild those quarters from the current source.
"""

from enum import Enum


class constants(Enum):
    DATASET_ID = "us_ffiec_bank_reporting"

    # Rebuilt by the quarterly flow. `dictionary` is listed here because it is
    # the dbt `ref()` target of the dictionary-coverage test on every other
    # table, so it has to exist before any of them are tested.
    QUARTERLY_TABLES = [
        "mdrm_item",
        "dictionary",
        "institution",
        "call_report_item",
        "holding_company",
        "holding_company_item",
    ]

    # Rebuilt by the annual CRA flow. `dictionary` is rebuilt here too, so that
    # neither flow depends on the other having bootstrapped it -- it is 121
    # static rows, and the two flows are scheduled hours apart so they cannot
    # race. See reference_shared_model_spans_two_flows for the failure this
    # avoids: a model whose staging sibling is owned by another flow cannot
    # compile until that flow has run at least once.
    CRA_TABLES = [
        "dictionary",
        "cra_respondent",
        "cra_lending",
        "cra_assessment_area_tract",
    ]

    # How many trailing quarters to rebuild. The FFIEC accepts amended Call
    # Reports for several quarters, and us_fdic_bankfind settled on 2 for the
    # same filings; 4 is a full year of restatement at a few minutes' extra
    # cost, since each quarter is one ~11 MB zip.
    TRAILING_QUARTERS = 4

    # How many trailing years of CRA to rebuild. The FFIEC reissues a year's
    # disclosure files when a respondent resubmits, so the most recent two are
    # rebuilt rather than assumed final.
    TRAILING_CRA_YEARS = 2

    # The table whose poll decides whether the quarterly flow has work to do.
    QUARTERLY_POLL_TABLE = "call_report_item"
    CRA_POLL_TABLE = "cra_lending"
