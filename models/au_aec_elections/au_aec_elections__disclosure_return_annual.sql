{{
    config(
        schema="au_aec_elections",
        alias="disclosure_return_annual",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1998, "end": 2035, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(financial_year as string) financial_year,
    safe_cast(return_type as string) return_type,
    safe_cast(name as string) name,
    safe_cast(lodged_on_behalf_of as string) lodged_on_behalf_of,
    safe_cast(party_group as string) party_group,
    safe_cast(associated_parties as string) associated_parties,
    safe_cast(client_type as string) client_type,
    safe_cast(client_file_id as string) client_file_id,
    safe_cast(abn as string) abn,
    safe_cast(acn as string) acn,
    safe_cast(total_receipts as float64) total_receipts,
    safe_cast(total_payments as float64) total_payments,
    safe_cast(total_debts as float64) total_debts,
    safe_cast(total_discretionary_benefits as float64) total_discretionary_benefits,
    safe_cast(capital_contributions as float64) capital_contributions,
    safe_cast(total_donations_made as float64) total_donations_made,
    safe_cast(total_donations_received as float64) total_donations_received,
    safe_cast(total_expenditure as float64) total_expenditure,
    safe_cast(electoral_expenditure as float64) electoral_expenditure,
    safe_cast(number_of_donors as int64) number_of_donors
from
    {{ set_datalake_project("au_aec_elections_staging.disclosure_return_annual") }} as t
