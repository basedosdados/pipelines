{{
    config(
        schema="au_apra_adi",
        alias="performance_ratios",
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
    safe_cast(net_interest_income as float64) net_interest_income,
    safe_cast(total_operating_income as float64) total_operating_income,
    safe_cast(total_operating_expenses as float64) total_operating_expenses,
    safe_cast(net_profit_after_tax as float64) net_profit_after_tax,
    safe_cast(average_total_assets as float64) average_total_assets,
    safe_cast(
        average_total_shareholders_equity as float64
    ) average_total_shareholders_equity,
    safe_cast(net_interest_income_to_assets as float64) net_interest_income_to_assets,
    safe_cast(operating_income_to_assets as float64) operating_income_to_assets,
    safe_cast(operating_expenses_to_assets as float64) operating_expenses_to_assets,
    safe_cast(profit_margin as float64) profit_margin,
    safe_cast(return_on_assets as float64) return_on_assets,
    safe_cast(return_on_equity as float64) return_on_equity,
    safe_cast(other_operating_income as float64) other_operating_income,
    safe_cast(fee_and_commission_income as float64) fee_and_commission_income,
    safe_cast(total_operating_income__2 as float64) total_operating_income__2,
    safe_cast(operating_expenses__2 as float64) operating_expenses__2,
    safe_cast(personnel_expenses as float64) personnel_expenses,
    safe_cast(non_interest_income_share as float64) non_interest_income_share,
    safe_cast(
        fee_income_to_total_operating_income as float64
    ) fee_income_to_total_operating_income,
    safe_cast(cost_to_income as float64) cost_to_income,
    safe_cast(
        personnel_to_operating_expenses as float64
    ) personnel_to_operating_expenses,
    safe_cast(total_assets as float64) total_assets,
    safe_cast(average_net_loans_and_advances as float64) average_net_loans_and_advances,
    safe_cast(average_deposits as float64) average_deposits,
    safe_cast(growth_in_total_assets as float64) growth_in_total_assets,
    safe_cast(net_loans_to_deposits as float64) net_loans_to_deposits,
    safe_cast(deposits_to_assets as float64) deposits_to_assets,
    safe_cast(equity_to_deposits as float64) equity_to_deposits,
    safe_cast(total_capital_base as float64) total_capital_base,
    safe_cast(total_risk_weighted_assets as float64) total_risk_weighted_assets,
    safe_cast(total_capital_ratio as float64) total_capital_ratio,
    safe_cast(total_impaired_facilities as float64) total_impaired_facilities,
    safe_cast(
        impaired_facilities_to_loans_and_advances as float64
    ) impaired_facilities_to_loans_and_advances,
    safe_cast(gross_loans_and_advances as float64) gross_loans_and_advances,
    safe_cast(total_non_performing_exposures as float64) total_non_performing_exposures,
    safe_cast(
        non_performing_to_loans_and_advances as float64
    ) non_performing_to_loans_and_advances,
    safe_cast(total_lcr_liquid_assets as float64) total_lcr_liquid_assets,
    safe_cast(net_cash_outflows as float64) net_cash_outflows,
    safe_cast(liquidity_coverage_ratio as float64) liquidity_coverage_ratio,
    safe_cast(
        total_adjusted_minimum_liquidity_holdings as float64
    ) total_adjusted_minimum_liquidity_holdings,
    safe_cast(adjusted_liability_base as float64) adjusted_liability_base,
    safe_cast(mlh_ratio as float64) mlh_ratio,
    safe_cast(number_of_entities as int64) number_of_entities,
    safe_cast(capital_ratio as float64) capital_ratio,
    safe_cast(total_loans_and_advances as float64) total_loans_and_advances
from {{ set_datalake_project("au_apra_adi_staging.performance_ratios") }} as t
