{{
    config(
        schema="au_apra_adi",
        alias="financial_position",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2004, "end": 2035, "interval": 1},
        },
        cluster_by=["institution_type"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(quarter as int64) quarter,
    safe_cast(institution_type as string) institution_type,
    safe_cast(cash_and_liquid_assets as float64) cash_and_liquid_assets,
    safe_cast(securities as float64) securities,
    safe_cast(acceptances_of_customers as float64) acceptances_of_customers,
    safe_cast(gross_loans_and_advances as float64) gross_loans_and_advances,
    safe_cast(total_housing as float64) total_housing,
    safe_cast(term as float64) term,
    safe_cast(
        gross_loans_and_advances__other as float64
    ) gross_loans_and_advances__other,
    safe_cast(lending_provisions as float64) lending_provisions,
    safe_cast(net_loans_and_advances as float64) net_loans_and_advances,
    safe_cast(fixed_assets as float64) fixed_assets,
    safe_cast(intangible_assets as float64) intangible_assets,
    safe_cast(other_assets as float64) other_assets,
    safe_cast(total_assets as float64) total_assets,
    safe_cast(acceptances as float64) acceptances,
    safe_cast(deposits as float64) deposits,
    safe_cast(call_on_demand as float64) call_on_demand,
    safe_cast(term_deposits as float64) term_deposits,
    safe_cast(certificates_of_deposit as float64) certificates_of_deposit,
    safe_cast(income_tax_liability as float64) income_tax_liability,
    safe_cast(provisions as float64) provisions,
    safe_cast(employee_entitlements as float64) employee_entitlements,
    safe_cast(provisions__other as float64) provisions__other,
    safe_cast(other_short_term_borrowings as float64) other_short_term_borrowings,
    safe_cast(long_term_borrowings as float64) long_term_borrowings,
    safe_cast(
        creditors_and_other_liabilities as float64
    ) creditors_and_other_liabilities,
    safe_cast(total_liabilities as float64) total_liabilities,
    safe_cast(share_capital as float64) share_capital,
    safe_cast(reserves as float64) reserves,
    safe_cast(retained_profits as float64) retained_profits,
    safe_cast(provisions__other__2 as float64) provisions__other__2,
    safe_cast(total_shareholders_equity as float64) total_shareholders_equity,
    safe_cast(number_of_entities as int64) number_of_entities,
    safe_cast(
        gross_loans_and_advances__intra_group as float64
    ) gross_loans_and_advances__intra_group,
    safe_cast(
        due_from_overseas_operations_of_the_adi as float64
    ) due_from_overseas_operations_of_the_adi,
    safe_cast(due_from_non_residents as float64) due_from_non_residents,
    safe_cast(deposits__intra_group as float64) deposits__intra_group,
    safe_cast(
        due_to_overseas_operations_of_the_adi as float64
    ) due_to_overseas_operations_of_the_adi,
    safe_cast(due_to_non_residents as float64) due_to_non_residents,
    safe_cast(other_investments as float64) other_investments,
    safe_cast(
        due_to_clearing_houses_and_financial_institutions as float64
    ) due_to_clearing_houses_and_financial_institutions
from {{ set_datalake_project("au_apra_adi_staging.financial_position") }} as t
