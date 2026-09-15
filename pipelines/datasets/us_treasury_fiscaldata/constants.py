"""Constants for the us_treasury_fiscaldata pipeline (Prefect 3).

U.S. Treasury FiscalData API — a keyless, paginated REST API. Eight data tables,
one per endpoint (the Monthly Treasury Statement split into four wide tables):

- ``debt_outstanding``             v2/accounting/od/debt_to_penny       (daily)
- ``historical_debt_outstanding``  v2/accounting/od/debt_outstanding    (annual)
- ``mts_summary``                  v1/accounting/mts/mts_table_1        (monthly)
- ``mts_receipts``                 v1/accounting/mts/mts_table_4        (monthly)
- ``mts_outlays``                  v1/accounting/mts/mts_table_5        (monthly)
- ``mts_means_of_financing``       v1/accounting/mts/mts_table_6        (monthly)
- ``average_interest_rate``        v2/accounting/od/avg_interest_rates  (monthly)
- ``exchange_rate``                v1/accounting/od/rates_of_exchange   (quarterly)

See models/us_treasury_fiscaldata/ONBOARDING_PLAN.md for the full design.
"""

from enum import Enum
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the us_treasury_fiscaldata pipeline.

    Lowercase class name follows the repo-wide convention. ``ARCHITECTURE_DIR``
    points at the committed architecture CSVs, the schema source of truth for
    both this pipeline and the one-shot bootstrap.
    """

    DATASET_ID = "us_treasury_fiscaldata"

    API_BASE = (
        "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
    )
    PAGE_SIZE = 10000  # API rejects page[size] > 10000

    # table slug -> list of endpoint paths. The Monthly Treasury Statement is
    # split into four wide tables, one per MTS table that carries distinct
    # actuals (T1 summary, T4 receipts, T5 outlays, T6 means of financing); the
    # overlapping summaries and estimate tables (2, 3, 7, 8, 9) are left out.
    ENDPOINTS = {
        "debt_outstanding": ["v2/accounting/od/debt_to_penny"],
        "historical_debt_outstanding": ["v2/accounting/od/debt_outstanding"],
        "mts_summary": ["v1/accounting/mts/mts_table_1"],
        "mts_receipts": ["v1/accounting/mts/mts_table_4"],
        "mts_outlays": ["v1/accounting/mts/mts_table_5"],
        "mts_means_of_financing": ["v1/accounting/mts/mts_table_6"],
        "average_interest_rate": ["v2/accounting/od/avg_interest_rates"],
        "exchange_rate": ["v1/accounting/od/rates_of_exchange"],
    }

    DATA_TABLES = [
        "debt_outstanding",
        "historical_debt_outstanding",
        "mts_summary",
        "mts_receipts",
        "mts_outlays",
        "mts_means_of_financing",
        "average_interest_rate",
        "exchange_rate",
    ]
    ALL_TABLES = (
        DATA_TABLES  # no dictionary: the wide tables have no coded columns
    )

    # Poll granularity per table (must match the coverage granularity).
    DATE_FORMAT = {
        "debt_outstanding": "%Y-%m-%d",
        "historical_debt_outstanding": "%Y",
        "mts_summary": "%Y-%m",
        "mts_receipts": "%Y-%m",
        "mts_outlays": "%Y-%m",
        "mts_means_of_financing": "%Y-%m",
        "average_interest_rate": "%Y-%m",
        "exchange_rate": "%Y-%m-%d",
    }

    # MTS wide tables: source amount field -> output column name. The common
    # key/hierarchy columns are handled uniformly in utils._clean_mts.
    MTS_AMOUNTS = {
        "mts_summary": {
            "current_month_gross_rcpt_amt": "current_month_gross_receipts",
            "current_month_gross_outly_amt": "current_month_gross_outlays",
            "current_month_dfct_sur_amt": "current_month_deficit_or_surplus",
        },
        "mts_receipts": {
            "current_month_gross_rcpt_amt": "current_month_gross_receipts",
            "current_month_refund_amt": "current_month_refunds",
            "current_month_net_rcpt_amt": "current_month_net_receipts",
            "current_fytd_gross_rcpt_amt": "current_fytd_gross_receipts",
            "current_fytd_refund_amt": "current_fytd_refunds",
            "current_fytd_net_rcpt_amt": "current_fytd_net_receipts",
            "prior_fytd_gross_rcpt_amt": "prior_fytd_gross_receipts",
            "prior_fytd_refund_amt": "prior_fytd_refunds",
            "prior_fytd_net_rcpt_amt": "prior_fytd_net_receipts",
        },
        "mts_outlays": {
            "current_month_gross_outly_amt": "current_month_gross_outlays",
            "current_month_app_rcpt_amt": "current_month_applicable_receipts",
            "current_month_net_outly_amt": "current_month_net_outlays",
            "current_fytd_gross_outly_amt": "current_fytd_gross_outlays",
            "current_fytd_app_rcpt_amt": "current_fytd_applicable_receipts",
            "current_fytd_net_outly_amt": "current_fytd_net_outlays",
            "prior_fytd_gross_outly_amt": "prior_fytd_gross_outlays",
            "prior_fytd_app_rcpt_amt": "prior_fytd_applicable_receipts",
            "prior_fytd_net_outly_amt": "prior_fytd_net_outlays",
        },
        "mts_means_of_financing": {
            "current_month_net_txn_amt": "current_month_net_transactions",
            "fytd_net_txn_amt": "current_fytd_net_transactions",
            "prior_fytd_net_txn_amt": "prior_fytd_net_transactions",
            "begin_year_acct_bal_amt": "beginning_year_balance",
            "begin_month_acct_bal_amt": "beginning_month_balance",
            "close_month_acct_bal_amt": "closing_month_balance",
        },
    }

    ARCHITECTURE_DIR = (
        _REPO_ROOT
        / "models"
        / "us_treasury_fiscaldata"
        / "code"
        / "architecture"
    )
