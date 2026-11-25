"""Constants for the us_cms_hcris pipeline.

HCRIS — the Healthcare Cost Report Information System — is the Medicare cost
report every Medicare-certified US hospital files once per fiscal year. CMS
publishes it as quarterly flat-file extracts, one set per **federal fiscal year
of receipt**, in two form versions that overlap in time:

* ``2552-96`` ("v1996"), extracts FY1996-FY2011, archive ``HOSPFY<year>.ZIP``
* ``2552-10`` ("v2010"), extracts FY2010 onwards, archive ``HOSP10FY<year>.ZIP``

Each archive holds headerless CSVs: ``rpt`` (one row per filed report),
``nmrc`` (numeric cell values) and ``alpha`` (text cell values). The v1996
archives also carry a ``rollup`` file, which this dataset does not ingest — see
``models/us_cms_hcris/CLAUDE.md``.
"""

from enum import Enum


class constants(Enum):
    """Constant values for the us_cms_hcris flows."""

    DATASET_ID = "us_cms_hcris"

    # Landing page listing every fiscal-year extract.
    SOURCE_PAGE = (
        "https://www.cms.gov/data-research/statistics-trends-and-reports/"
        "cost-reports/cost-reports-fiscal-year"
    )
    DOWNLOAD_BASE = "https://downloads.cms.gov/FILES/HCRIS"

    # Form version -> (archive stem template, CSV stem template, first extract
    # year, last extract year). The end years are probed at run time; these are
    # the bounds the onboarding was built and verified against.
    FORMS = {
        "2552-96": {
            "zip": "HOSPFY{year}.ZIP",
            "csv": "HOSP_{year}_{part}.csv",
            "first_year": 1996,
        },
        "2552-10": {
            "zip": "HOSP10FY{year}.ZIP",
            "csv": "HOSP10_{year}_{part}.csv",
            "first_year": 2010,
        },
    }

    # The 18 columns of the RPT record, in source order. Headerless in the CSV.
    RPT_COLUMNS = [
        "rpt_rec_num",
        "prvdr_ctrl_type_cd",
        "prvdr_num",
        "npi",
        "rpt_stus_cd",
        "fy_bgn_dt",
        "fy_end_dt",
        "proc_dt",
        "initl_rpt_sw",
        "last_rpt_sw",
        "trnsmtl_num",
        "fi_num",
        "adr_vndr_cd",
        "fi_creat_dt",
        "util_cd",
        "npr_dt",
        "spec_ind",
        "fi_rcpt_dt",
    ]

    # NMRC and ALPHA share a layout: the cell address plus one value.
    CELL_COLUMNS = [
        "rpt_rec_num",
        "wksht_cd",
        "line_num",
        "clmn_num",
        "item_value",
    ]

    ALL_TABLES = ["report", "report_value", "hospital_financial", "dicionario"]
    # Tables built by the cleaning transform and uploaded to staging. The other
    # two are dbt models derived from these.
    STAGED_TABLES = ["report", "report_value"]

    # Earliest and latest fiscal-year-end year the partitions can take. Reports
    # are filed for fiscal years ending well before the extract that carries
    # them, so the partition range is wider than the extract range.
    PARTITION_START = 1994
    PARTITION_END = 2031
