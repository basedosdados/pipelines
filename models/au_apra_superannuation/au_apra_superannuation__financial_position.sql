{{
    config(
        schema="au_apra_superannuation",
        alias="financial_position",
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
    safe_cast(receivables as float64) receivables,
    safe_cast(investments as float64) investments,
    safe_cast(
        securities_purchased_under_resale_agreements as float64
    ) securities_purchased_under_resale_agreements,
    safe_cast(tax_assets as float64) tax_assets,
    safe_cast(other_assets as float64) other_assets,
    safe_cast(total_assets as float64) total_assets,
    safe_cast(
        securities_sold_under_repurchase_agreements as float64
    ) securities_sold_under_repurchase_agreements,
    safe_cast(tax_liabilities as float64) tax_liabilities,
    safe_cast(other_liabilities as float64) other_liabilities,
    safe_cast(total_liabilities as float64) total_liabilities,
    safe_cast(
        liability_for_allocated_accrued_benefits as float64
    ) liability_for_allocated_accrued_benefits,
    safe_cast(liability_for_members_benefits as float64) liability_for_members_benefits,
    safe_cast(
        defined_contribution_members_benefits as float64
    ) defined_contribution_members_benefits,
    safe_cast(
        defined_benefit_members_benefits as float64
    ) defined_benefit_members_benefits,
    safe_cast(unallocated_benefits as float64) unallocated_benefits,
    safe_cast(
        reserves_including_unallocated_benefits as float64
    ) reserves_including_unallocated_benefits,
    safe_cast(reserves as float64) reserves,
    safe_cast(excess_deficiency_of_assets as float64) excess_deficiency_of_assets,
    safe_cast(surplus_deficit_in_net_assets as float64) surplus_deficit_in_net_assets,
    safe_cast(
        net_assets_available_to_pay_benefits as float64
    ) net_assets_available_to_pay_benefits,
    safe_cast(defined_benefit_interests as float64) defined_benefit_interests,
    safe_cast(number_of_entities as int64) number_of_entities
from
    {{ set_datalake_project("au_apra_superannuation_staging.financial_position") }} as t
