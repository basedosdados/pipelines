#!/usr/bin/env python3
"""Write the architecture CSVs for us_treasury_fiscaldata — the schema source of
truth (column order + BigQuery type per table) shared by the one-shot bootstrap
and the recurring Prefect pipeline.

The FiscalData API publishes a field dictionary with declared data types per
endpoint (``meta.dataTypes``). The mapping to BigQuery types is:

    DATE       -> DATE
    CURRENCY   -> FLOAT64 (USD)
    PERCENTAGE -> FLOAT64 (percent)
    NUMBER     -> FLOAT64
    INTEGER    -> STRING   (src_line_nbr is a line number, not a quantity)
    YEAR       -> INT64    (year/fiscal_year, kept as analytical dimensions)

Type follows arithmetic meaning, not raw storage (see
.claude/rules/bigquery-conventions.md): coded/label columns stay STRING.

Usage:
    uv run python models/us_treasury_fiscaldata/code/build_architecture.py
"""

import csv
from pathlib import Path

ARCH = Path(__file__).resolve().parent / "architecture"

HEADER = [
    "name",
    "bigquery_type",
    "description",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "original_name",
]

# Each column: (name, type, description_en, covered_by_dict, directory_column,
# measurement_unit, observations, original_name)
_YEAR_DIR = "br_bd_diretorios_data_tempo.ano:ano"
_MONTH_DIR = "br_bd_diretorios_data_tempo.mes:mes"

TABLES: dict[str, list[tuple]] = {
    "debt_outstanding": [
        (
            "year",
            "INT64",
            "Calendar year of the record date",
            "no",
            _YEAR_DIR,
            "year",
            "Partition column derived from record_date",
            "record_date",
        ),
        (
            "record_date",
            "DATE",
            "Date the debt figure was reported, published on business days",
            "no",
            "",
            "",
            "Daily business-day series",
            "record_date",
        ),
        (
            "total_public_debt_outstanding",
            "FLOAT64",
            "Total public debt outstanding on the record date, the sum of debt held by the public and intragovernmental holdings",
            "no",
            "",
            "USD",
            "",
            "tot_pub_debt_out_amt",
        ),
        (
            "debt_held_by_public",
            "FLOAT64",
            "Portion of the total public debt held by the public rather than by government accounts",
            "no",
            "",
            "USD",
            "Reported from 1997 onward, null before",
            "debt_held_public_amt",
        ),
        (
            "intragovernmental_holdings",
            "FLOAT64",
            "Portion of the total public debt held by government trust funds, revolving funds, and special funds",
            "no",
            "",
            "USD",
            "Reported from 1997 onward, null before",
            "intragov_hold_amt",
        ),
    ],
    "historical_debt_outstanding": [
        (
            "year",
            "INT64",
            "Calendar year of the record date",
            "no",
            _YEAR_DIR,
            "year",
            "Partition column derived from record_date",
            "record_date",
        ),
        (
            "record_date",
            "DATE",
            "Date the debt figure reports, the last day of the fiscal year",
            "no",
            "",
            "",
            "Annual fiscal-year-end series from 1790",
            "record_date",
        ),
        (
            "fiscal_year",
            "INT64",
            "U.S. federal fiscal year the debt figure reports",
            "no",
            "",
            "year",
            "",
            "record_fiscal_year",
        ),
        (
            "debt_outstanding",
            "FLOAT64",
            "Total public debt outstanding of the U.S. government at the end of the fiscal year",
            "no",
            "",
            "USD",
            "High-level summary with no breakdown of debt components",
            "debt_outstanding_amt",
        ),
    ],
    "average_interest_rate": [
        (
            "year",
            "INT64",
            "Calendar year of the record date",
            "no",
            _YEAR_DIR,
            "year",
            "Partition column derived from record_date",
            "record_date",
        ),
        (
            "month",
            "INT64",
            "Calendar month of the record date",
            "no",
            _MONTH_DIR,
            "month",
            "Derived from record_date",
            "record_date",
        ),
        (
            "record_date",
            "DATE",
            "Last day of the month the average interest rate reports",
            "no",
            "",
            "",
            "Monthly series",
            "record_date",
        ),
        (
            "fiscal_year",
            "INT64",
            "U.S. federal fiscal year of the record date, running October through September",
            "no",
            "",
            "year",
            "",
            "record_fiscal_year",
        ),
        (
            "security_type",
            "STRING",
            "Broad Treasury security class, such as Marketable, Non-marketable, or Interest-bearing Debt",
            "no",
            "",
            "",
            "Human-readable label",
            "security_type_desc",
        ),
        (
            "security_desc",
            "STRING",
            "Specific Treasury security the rate applies to, such as Treasury Notes, Treasury Bonds, or Savings Bonds",
            "no",
            "",
            "",
            "Human-readable label",
            "security_desc",
        ),
        (
            "avg_interest_rate",
            "FLOAT64",
            "Average interest rate the Treasury pays on the outstanding security, as an annual percentage",
            "no",
            "",
            "percent",
            "",
            "avg_interest_rate_amt",
        ),
    ],
    "exchange_rate": [
        (
            "year",
            "INT64",
            "Calendar year of the record date",
            "no",
            _YEAR_DIR,
            "year",
            "Partition column derived from record_date",
            "record_date",
        ),
        (
            "record_date",
            "DATE",
            "Last day of the quarter the exchange rate reports",
            "no",
            "",
            "",
            "Quarterly Treasury reporting rate",
            "record_date",
        ),
        (
            "effective_date",
            "DATE",
            "Date the exchange rate takes effect for reporting purposes",
            "no",
            "",
            "",
            "May differ from record_date when a rate is revised mid-quarter",
            "effective_date",
        ),
        (
            "country",
            "STRING",
            "Country whose currency the rate converts",
            "no",
            "",
            "",
            "Free-text English country name; the source carries no ISO code, so no directory link",
            "country",
        ),
        (
            "currency",
            "STRING",
            "Currency the rate converts to U.S. dollars",
            "no",
            "",
            "",
            "Human-readable label",
            "currency",
        ),
        (
            "country_currency_desc",
            "STRING",
            "Country and currency combined, as published by the Treasury",
            "no",
            "",
            "",
            "Unique with record_date; some countries report more than one currency",
            "country_currency_desc",
        ),
        (
            "exchange_rate",
            "FLOAT64",
            "Units of the foreign currency per one U.S. dollar, the Treasury reporting rate",
            "no",
            "",
            "",
            "Unit varies by row (foreign currency per USD), so no single column-level measurement unit applies",
            "exchange_rate",
        ),
    ],
}

# The four Monthly Treasury Statement wide tables share one key/hierarchy
# skeleton and differ only in their dollar-amount columns. Built here from a
# common prefix + per-table amount specs so the schema stays DRY.
_MTS_COMMON = [
    (
        "year",
        "INT64",
        "Calendar year of the record date",
        "no",
        _YEAR_DIR,
        "year",
        "Partition column derived from record_date",
        "record_date",
    ),
    (
        "month",
        "INT64",
        "Calendar month of the record date",
        "no",
        _MONTH_DIR,
        "month",
        "Derived from record_date",
        "record_date",
    ),
    (
        "record_date",
        "DATE",
        "Last day of the month the statement reports",
        "no",
        "",
        "",
        "MTS is a monthly report",
        "record_date",
    ),
    (
        "fiscal_year",
        "INT64",
        "U.S. federal fiscal year of the record date, running October through September",
        "no",
        "",
        "year",
        "",
        "record_fiscal_year",
    ),
    (
        "src_line_nbr",
        "STRING",
        "Source line number within the MTS table, stable across releases for a given line",
        "no",
        "",
        "",
        "Part of the natural key (record_date, src_line_nbr)",
        "src_line_nbr",
    ),
    (
        "parent_id",
        "STRING",
        "Classification id of the parent row in the statement hierarchy, empty for top-level rows",
        "no",
        "",
        "",
        "Pairs with classification_id to reconstruct the tree within a release",
        "parent_id",
    ),
    (
        "classification_id",
        "STRING",
        "Classification id of the row within a single release, not stable across releases",
        "no",
        "",
        "",
        "Regenerated each release; use src_line_nbr as the stable key",
        "classification_id",
    ),
    (
        "classification_desc",
        "STRING",
        "Name of the receipt source, spending agency, budget function, or financing category the row reports",
        "no",
        "",
        "",
        "Human-readable label",
        "classification_desc",
    ),
    (
        "sequence_level_nbr",
        "STRING",
        "Depth of the row in the statement hierarchy, from 1 at the top to deeper components",
        "no",
        "",
        "",
        "Distinguishes totals from their components",
        "sequence_level_nbr",
    ),
]


def _amt(name, desc, orig):
    return (name, "FLOAT64", desc, "no", "", "USD", "", orig)


_MTS_TABLES = {
    "mts_summary": [
        _amt(
            "current_month_gross_receipts",
            "Total receipts of the U.S. government in the current month",
            "current_month_gross_rcpt_amt",
        ),
        _amt(
            "current_month_gross_outlays",
            "Total outlays of the U.S. government in the current month",
            "current_month_gross_outly_amt",
        ),
        _amt(
            "current_month_deficit_or_surplus",
            "Budget deficit (negative) or surplus (positive) in the current month",
            "current_month_dfct_sur_amt",
        ),
    ],
    "mts_receipts": [
        _amt(
            "current_month_gross_receipts",
            "Gross receipts from the source in the current month",
            "current_month_gross_rcpt_amt",
        ),
        _amt(
            "current_month_refunds",
            "Refunds of receipts from the source in the current month",
            "current_month_refund_amt",
        ),
        _amt(
            "current_month_net_receipts",
            "Net receipts from the source in the current month",
            "current_month_net_rcpt_amt",
        ),
        _amt(
            "current_fytd_gross_receipts",
            "Gross receipts from the source in the current fiscal year to date",
            "current_fytd_gross_rcpt_amt",
        ),
        _amt(
            "current_fytd_refunds",
            "Refunds of receipts from the source in the current fiscal year to date",
            "current_fytd_refund_amt",
        ),
        _amt(
            "current_fytd_net_receipts",
            "Net receipts from the source in the current fiscal year to date",
            "current_fytd_net_rcpt_amt",
        ),
        _amt(
            "prior_fytd_gross_receipts",
            "Gross receipts from the source in the prior fiscal year to date",
            "prior_fytd_gross_rcpt_amt",
        ),
        _amt(
            "prior_fytd_refunds",
            "Refunds of receipts from the source in the prior fiscal year to date",
            "prior_fytd_refund_amt",
        ),
        _amt(
            "prior_fytd_net_receipts",
            "Net receipts from the source in the prior fiscal year to date",
            "prior_fytd_net_rcpt_amt",
        ),
    ],
    "mts_outlays": [
        _amt(
            "current_month_gross_outlays",
            "Gross outlays of the agency in the current month",
            "current_month_gross_outly_amt",
        ),
        _amt(
            "current_month_applicable_receipts",
            "Receipts applied against the agency's outlays in the current month",
            "current_month_app_rcpt_amt",
        ),
        _amt(
            "current_month_net_outlays",
            "Net outlays of the agency in the current month",
            "current_month_net_outly_amt",
        ),
        _amt(
            "current_fytd_gross_outlays",
            "Gross outlays of the agency in the current fiscal year to date",
            "current_fytd_gross_outly_amt",
        ),
        _amt(
            "current_fytd_applicable_receipts",
            "Receipts applied against the agency's outlays in the current fiscal year to date",
            "current_fytd_app_rcpt_amt",
        ),
        _amt(
            "current_fytd_net_outlays",
            "Net outlays of the agency in the current fiscal year to date",
            "current_fytd_net_outly_amt",
        ),
        _amt(
            "prior_fytd_gross_outlays",
            "Gross outlays of the agency in the prior fiscal year to date",
            "prior_fytd_gross_outly_amt",
        ),
        _amt(
            "prior_fytd_applicable_receipts",
            "Receipts applied against the agency's outlays in the prior fiscal year to date",
            "prior_fytd_app_rcpt_amt",
        ),
        _amt(
            "prior_fytd_net_outlays",
            "Net outlays of the agency in the prior fiscal year to date",
            "prior_fytd_net_outly_amt",
        ),
    ],
    "mts_means_of_financing": [
        _amt(
            "current_month_net_transactions",
            "Net financing transactions in the current month",
            "current_month_net_txn_amt",
        ),
        _amt(
            "current_fytd_net_transactions",
            "Net financing transactions in the current fiscal year to date",
            "fytd_net_txn_amt",
        ),
        _amt(
            "prior_fytd_net_transactions",
            "Net financing transactions in the prior fiscal year to date",
            "prior_fytd_net_txn_amt",
        ),
        _amt(
            "beginning_year_balance",
            "Account balance at the beginning of the fiscal year",
            "begin_year_acct_bal_amt",
        ),
        _amt(
            "beginning_month_balance",
            "Account balance at the beginning of the month",
            "begin_month_acct_bal_amt",
        ),
        _amt(
            "closing_month_balance",
            "Account balance at the close of the month",
            "close_month_acct_bal_amt",
        ),
    ],
}
for _t, _amts in _MTS_TABLES.items():
    TABLES[_t] = _MTS_COMMON + _amts


def main():
    ARCH.mkdir(parents=True, exist_ok=True)
    for table, cols in TABLES.items():
        with open(ARCH / f"{table}.csv", "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(HEADER)
            for name, typ, desc, cbd, dircol, unit, obs, orig in cols:
                w.writerow(
                    [name, typ, desc, "", cbd, dircol, unit, "no", obs, orig]
                )
        print(f"{table}: {len(cols)} columns -> architecture/{table}.csv")


if __name__ == "__main__":
    main()
