#!/usr/bin/env python3
"""Generate the architecture CSVs for au_apra_superannuation from the measure
mapping in the pipeline's utils.py plus the English descriptions below.

The architecture CSV is the schema source of truth (column order + types read by
the transform and the dbt models). English descriptions live here; Portuguese and
Spanish translations are attached by build_columns_json.py.

Usage:
    uv run python models/au_apra_superannuation/code/build_architecture.py
"""

import csv
from pathlib import Path

from pipelines.datasets.au_apra_superannuation.utils import MEASURES

ARCH = Path(__file__).resolve().parent / "architecture"

# unit token -> measurement_unit written to the CSV
UNIT = {
    "aud_million": "AUD million",
    "proportion": "proportion",
    "number": "unit",
}

# English description per measure column (shared across statements where meaning
# is identical). Amounts are in millions of AUD; that is stated in observations.
DESC = {
    # performance
    "net_assets_beginning": "Net assets available to pay benefits at the beginning of the quarter",
    "total_contributions": "Total contributions received during the quarter",
    "employer_contributions": "Employer contributions received during the quarter",
    "employer_defined_benefit_contributions": "Employer contributions to defined benefit interests during the quarter",
    "employer_super_guarantee_contributions": "Employer contributions made under the compulsory Superannuation Guarantee during the quarter",
    "employer_salary_sacrifice_contributions": "Employer contributions made through salary sacrifice arrangements during the quarter",
    "member_contributions": "Member contributions received during the quarter",
    "member_personal_contributions": "Personal contributions made by members during the quarter",
    "government_co_contributions": "Government co-contributions received during the quarter",
    "low_income_super_contributions": "Low income superannuation contributions received during the quarter",
    "other_member_contributions": "Other member contributions received during the quarter",
    "contribution_tax_and_surcharge": "Contributions tax and surcharge for the quarter",
    "net_benefit_transfers": "Net benefit transfers into or out of the fund type during the quarter",
    "net_rollovers_to_from_smsf": "Net rollovers to or from self-managed superannuation funds during the quarter",
    "benefit_transfers_inward": "Benefit transfers received (inward) during the quarter",
    "benefit_transfers_outward": "Benefit transfers paid out (outward) during the quarter",
    "benefit_payments": "Total benefit payments made during the quarter",
    "lump_sum_benefits": "Lump sum benefit payments made during the quarter",
    "pension_benefits": "Pension benefit payments made during the quarter",
    "other_members_benefit_flows": "Other members' benefit flows during the quarter",
    "net_contribution_flows": "Net contribution flows during the quarter (contributions and transfers net of benefit payments)",
    "net_insurance_flows": "Net insurance flows during the quarter",
    "insurance_flows_inward": "Insurance flows received (inward) during the quarter",
    "insurance_flows_outward": "Insurance flows paid out (outward) during the quarter",
    "investment_income": "Investment income earned during the quarter",
    "investment_income_after_impairment": "Investment income after impairment expense during the quarter",
    "total_gains_losses_on_investments": "Total realised and unrealised gains or losses on investments during the quarter",
    "foreign_exchange_gains_losses": "Foreign exchange gains or losses during the quarter",
    "investment_expenses": "Investment expenses incurred during the quarter",
    "operating_income": "Operating income during the quarter",
    "administration_and_operating_expenses": "Administration and operating expenses during the quarter",
    "net_earnings": "Net earnings during the quarter",
    "income_tax_expense_benefit": "Income tax expense or benefit for the quarter",
    "net_earnings_after_tax": "Net earnings after tax during the quarter",
    "net_operating_performance_after_tax": "Net operating performance after tax during the quarter",
    "other_changes": "Other changes in net assets during the quarter",
    "net_assets_end": "Net assets available to pay benefits at the end of the quarter",
    "number_of_entities": "Number of superannuation entities in the fund type",
    # position
    "receivables": "Receivables held at the end of the quarter",
    "investments": "Total investments held at the end of the quarter",
    "securities_purchased_under_resale_agreements": "Securities purchased under agreements to resell and securities borrowed, at the end of the quarter",
    "tax_assets": "Tax assets held at the end of the quarter",
    "other_assets": "Other assets held at the end of the quarter",
    "total_assets": "Total assets held at the end of the quarter",
    "securities_sold_under_repurchase_agreements": "Securities sold under agreements to repurchase and securities loaned, at the end of the quarter",
    "tax_liabilities": "Tax liabilities at the end of the quarter",
    "other_liabilities": "Other liabilities at the end of the quarter",
    "total_liabilities": "Total liabilities at the end of the quarter",
    "liability_for_allocated_accrued_benefits": "Liability for allocated accrued benefits at the end of the quarter",
    "liability_for_members_benefits": "Liability for members' benefits at the end of the quarter",
    "defined_contribution_members_benefits": "Defined contribution members' benefits at the end of the quarter",
    "defined_benefit_members_benefits": "Defined benefit members' benefits at the end of the quarter",
    "unallocated_benefits": "Unallocated benefits at the end of the quarter",
    "reserves_including_unallocated_benefits": "Reserves including unallocated benefits at the end of the quarter",
    "reserves": "Reserves at the end of the quarter",
    "excess_deficiency_of_assets": "Excess or deficiency of assets at the end of the quarter",
    "surplus_deficit_in_net_assets": "Surplus or deficit in net assets at the end of the quarter",
    "net_assets_available_to_pay_benefits": "Net assets available to pay members' benefits at the end of the quarter",
    "defined_benefit_interests": "Net assets attributable to defined benefit interests at the end of the quarter",
    # ratios
    "net_cash_flows": "Net cash flows during the quarter, an input to the rate-of-return calculation",
    "cash_flow_adjusted_net_assets": "Cash-flow adjusted net assets, an input to the rate-of-return calculation",
    "investment_expense": "Investment expense used in the rate-of-return calculation",
    "administration_and_operating_expense": "Administration and operating expense used in the rate-of-return calculation",
    "rate_of_return": "Quarterly rate of return, expressed as a proportion (e.g. 0.056 = 5.6%)",
    "five_year_annualised_rate_of_return": "Five-year annualised rate of return, expressed as a proportion",
}

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

KEY_ROWS = [
    {
        "name": "year",
        "bigquery_type": "INT64",
        "description": "Reference year of the quarter-end observation",
        "temporal_coverage": "",
        "covered_by_dictionary": "no",
        "directory_column": "br_bd_diretorios_data_tempo.ano:ano",
        "measurement_unit": "year",
        "has_sensitive_data": "no",
        "observations": "Partition column",
        "original_name": "",
    },
    {
        "name": "quarter",
        "bigquery_type": "INT64",
        "description": "Reference quarter of the observation, from 1 to 4",
        "temporal_coverage": "",
        "covered_by_dictionary": "no",
        "directory_column": "",
        "measurement_unit": "quarter",
        "has_sensitive_data": "no",
        "observations": "APRA labels each quarter by its final month: Q1 March, Q2 June, Q3 September, Q4 December",
        "original_name": "",
    },
    {
        "name": "fund_type",
        "bigquery_type": "STRING",
        "description": "Superannuation fund type: all (whole industry), corporate, industry, public sector or retail funds",
        "temporal_coverage": "",
        "covered_by_dictionary": "yes",
        "directory_column": "",
        "measurement_unit": "",
        "has_sensitive_data": "no",
        "observations": "APRA regulates only these fund types; self-managed super funds (SMSFs) are regulated by the ATO and are not covered",
        "original_name": "",
    },
]

DIC_ROWS = [
    (
        "id_tabela",
        "Slug of the au_apra_superannuation table the dictionary entry describes",
    ),
    ("nome_coluna", "Name of the column the dictionary entry describes"),
    ("chave", "Coded value (key) exactly as stored in the data"),
    ("cobertura_temporal", "Temporal coverage of the key"),
    ("valor", "Human-readable label corresponding to the coded value"),
]


def measure_row(col, unit, original):
    obs = (
        "Values in millions of Australian dollars"
        if unit == "aud_million"
        else ""
    )
    if unit == "proportion":
        obs = "Dimensionless proportion; APRA labels it a percentage but stores a fraction"
    return {
        "name": col,
        "bigquery_type": "INT64" if unit == "number" else "FLOAT64",
        "description": DESC[col],
        "temporal_coverage": "",
        "covered_by_dictionary": "no",
        "directory_column": "",
        "measurement_unit": UNIT[unit],
        "has_sensitive_data": "no",
        "observations": obs,
        "original_name": original,
    }


def write_csv(name, rows):
    ARCH.mkdir(parents=True, exist_ok=True)
    with open(ARCH / f"{name}.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=HEADER)
        w.writeheader()
        w.writerows(rows)
    print(f"{name}: {len(rows)} columns -> architecture/{name}.csv")


def main():
    for table, spec in MEASURES.items():
        rows = [dict(r) for r in KEY_ROWS]
        for label, col, unit in spec:
            if col is None:
                continue
            rows.append(measure_row(col, unit, label))
        write_csv(table, rows)
    dic = [
        {
            "name": n,
            "bigquery_type": "STRING",
            "description": d,
            "temporal_coverage": "",
            "covered_by_dictionary": "no",
            "directory_column": "",
            "measurement_unit": "",
            "has_sensitive_data": "no",
            "observations": "",
            "original_name": n,
        }
        for n, d in DIC_ROWS
    ]
    write_csv("dicionario", dic)


if __name__ == "__main__":
    main()
