{{
    config(
        schema="au_apra_adi",
        alias="financial_performance",
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
    safe_cast(interest_income as float64) interest_income,
    safe_cast(cash_and_liquid_assets as float64) cash_and_liquid_assets,
    safe_cast(loans_and_advances as float64) loans_and_advances,
    safe_cast(housing_loans as float64) housing_loans,
    safe_cast(term_loans as float64) term_loans,
    safe_cast(interest_income__other as float64) interest_income__other,
    safe_cast(other_interest_earning_assets as float64) other_interest_earning_assets,
    safe_cast(interest_expense as float64) interest_expense,
    safe_cast(deposits as float64) deposits,
    safe_cast(borrowings as float64) borrowings,
    safe_cast(
        other_interest_bearing_liabilities as float64
    ) other_interest_bearing_liabilities,
    safe_cast(net_interest_income as float64) net_interest_income,
    safe_cast(other_operating_income as float64) other_operating_income,
    safe_cast(fee_and_commission as float64) fee_and_commission,
    safe_cast(lending as float64) lending,
    safe_cast(
        transaction_deposit_account_service_fee as float64
    ) transaction_deposit_account_service_fee,
    safe_cast(other_fee_based_activities as float64) other_fee_based_activities,
    safe_cast(other_operating_income__other as float64) other_operating_income__other,
    safe_cast(total_operating_income as float64) total_operating_income,
    safe_cast(
        charge_for_bad_or_doubtful_debts as float64
    ) charge_for_bad_or_doubtful_debts,
    safe_cast(total_operating_expenses as float64) total_operating_expenses,
    safe_cast(personnel as float64) personnel,
    safe_cast(fees_and_commissions as float64) fees_and_commissions,
    safe_cast(
        total_operating_expenses__other as float64
    ) total_operating_expenses__other,
    safe_cast(profit_before_tax as float64) profit_before_tax,
    safe_cast(income_tax as float64) income_tax,
    safe_cast(net_profit_after_taxa as float64) net_profit_after_taxa,
    safe_cast(number_of_entities as int64) number_of_entities,
    safe_cast(operating_expenses__other as float64) operating_expenses__other
from {{ set_datalake_project("au_apra_adi_staging.financial_performance") }} as t
