{{
    config(
        schema="au_apra_superannuation",
        alias="financial_performance",
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
    safe_cast(total_contributions as float64) total_contributions,
    safe_cast(employer_contributions as float64) employer_contributions,
    safe_cast(
        employer_defined_benefit_contributions as float64
    ) employer_defined_benefit_contributions,
    safe_cast(
        employer_super_guarantee_contributions as float64
    ) employer_super_guarantee_contributions,
    safe_cast(
        employer_salary_sacrifice_contributions as float64
    ) employer_salary_sacrifice_contributions,
    safe_cast(member_contributions as float64) member_contributions,
    safe_cast(member_personal_contributions as float64) member_personal_contributions,
    safe_cast(government_co_contributions as float64) government_co_contributions,
    safe_cast(low_income_super_contributions as float64) low_income_super_contributions,
    safe_cast(other_member_contributions as float64) other_member_contributions,
    safe_cast(contribution_tax_and_surcharge as float64) contribution_tax_and_surcharge,
    safe_cast(net_benefit_transfers as float64) net_benefit_transfers,
    safe_cast(net_rollovers_to_from_smsf as float64) net_rollovers_to_from_smsf,
    safe_cast(benefit_transfers_inward as float64) benefit_transfers_inward,
    safe_cast(benefit_transfers_outward as float64) benefit_transfers_outward,
    safe_cast(benefit_payments as float64) benefit_payments,
    safe_cast(lump_sum_benefits as float64) lump_sum_benefits,
    safe_cast(pension_benefits as float64) pension_benefits,
    safe_cast(other_members_benefit_flows as float64) other_members_benefit_flows,
    safe_cast(net_contribution_flows as float64) net_contribution_flows,
    safe_cast(net_insurance_flows as float64) net_insurance_flows,
    safe_cast(insurance_flows_inward as float64) insurance_flows_inward,
    safe_cast(insurance_flows_outward as float64) insurance_flows_outward,
    safe_cast(investment_income as float64) investment_income,
    safe_cast(
        investment_income_after_impairment as float64
    ) investment_income_after_impairment,
    safe_cast(
        total_gains_losses_on_investments as float64
    ) total_gains_losses_on_investments,
    safe_cast(foreign_exchange_gains_losses as float64) foreign_exchange_gains_losses,
    safe_cast(investment_expenses as float64) investment_expenses,
    safe_cast(operating_income as float64) operating_income,
    safe_cast(
        administration_and_operating_expenses as float64
    ) administration_and_operating_expenses,
    safe_cast(net_earnings as float64) net_earnings,
    safe_cast(income_tax_expense_benefit as float64) income_tax_expense_benefit,
    safe_cast(net_earnings_after_tax as float64) net_earnings_after_tax,
    safe_cast(
        net_operating_performance_after_tax as float64
    ) net_operating_performance_after_tax,
    safe_cast(other_changes as float64) other_changes,
    safe_cast(net_assets_end as float64) net_assets_end,
    safe_cast(number_of_entities as int64) number_of_entities
from
    {{ set_datalake_project("au_apra_superannuation_staging.financial_performance") }}
    as t
