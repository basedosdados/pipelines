{{
    config(
        schema="au_apra_superannuation",
        alias="performance_ratios",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2004, "end": 2035, "interval": 1},
        },
        cluster_by=["fund_type"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(quarter as int64) quarter,
    safe_cast(fund_type as string) fund_type,
    safe_cast(net_assets_beginning as float64) net_assets_beginning,
    safe_cast(net_cash_flows as float64) net_cash_flows,
    safe_cast(cash_flow_adjusted_net_assets as float64) cash_flow_adjusted_net_assets,
    safe_cast(investment_income as float64) investment_income,
    safe_cast(investment_expense as float64) investment_expense,
    safe_cast(operating_income as float64) operating_income,
    safe_cast(
        administration_and_operating_expense as float64
    ) administration_and_operating_expense,
    safe_cast(income_tax_expense_benefit as float64) income_tax_expense_benefit,
    safe_cast(net_earnings_after_tax as float64) net_earnings_after_tax,
    safe_cast(rate_of_return as float64) rate_of_return,
    safe_cast(
        five_year_annualised_rate_of_return as float64
    ) five_year_annualised_rate_of_return,
    safe_cast(number_of_entities as int64) number_of_entities
from
    {{ set_datalake_project("au_apra_superannuation_staging.performance_ratios") }} as t
