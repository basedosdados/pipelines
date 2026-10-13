{{
    config(
        schema="us_irs_form990",
        alias="return_financial",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2010, "end": 2040, "interval": 1},
        },
    )
}}

-- Atualizado em 2026-09-03
-- The IRS releases amended returns and occasionally re-releases a
-- filing in a later batch. One return is kept per (ein, year,
-- form_type): the most recently filed, ties broken by object_id, so
-- re-loading a batch into staging never duplicates a row.
with
    staged as (
        select
            safe_cast(year as int64) year,
            safe_cast(ein as string) ein,
            safe_cast(form_type as string) form_type,
            safe_cast(object_id as string) object_id,
            safe_cast(return_version as string) return_version,
            safe_cast(xml_batch_id as string) xml_batch_id,
            safe_cast(return_timestamp as timestamp) return_timestamp,
            safe_cast(tax_period_begin as date) tax_period_begin,
            safe_cast(tax_period_end as date) tax_period_end,
            safe_cast(is_amended as boolean) is_amended,
            safe_cast(is_initial as boolean) is_initial,
            safe_cast(is_final as boolean) is_final,
            safe_cast(is_group_return as boolean) is_group_return,
            safe_cast(organization_name as string) organization_name,
            safe_cast(doing_business_as_name as string) doing_business_as_name,
            safe_cast(principal_officer_name as string) principal_officer_name,
            safe_cast(address_line_1 as string) address_line_1,
            safe_cast(city as string) city,
            safe_cast(state as string) state,
            safe_cast(zip_code as string) zip_code,
            safe_cast(country as string) country,
            safe_cast(website as string) website,
            safe_cast(formation_year as int64) formation_year,
            safe_cast(legal_domicile_state as string) legal_domicile_state,
            safe_cast(organization_type as string) organization_type,
            safe_cast(exempt_status as string) exempt_status,
            safe_cast(group_exemption_number as string) group_exemption_number,
            safe_cast(mission_description as string) mission_description,
            safe_cast(gross_receipts as float64) gross_receipts,
            safe_cast(voting_members_count as int64) voting_members_count,
            safe_cast(
                independent_voting_members_count as int64
            ) independent_voting_members_count,
            safe_cast(employees_count as int64) employees_count,
            safe_cast(volunteers_count as int64) volunteers_count,
            safe_cast(unrelated_business_revenue as float64) unrelated_business_revenue,
            safe_cast(
                unrelated_business_taxable_income as float64
            ) unrelated_business_taxable_income,
            safe_cast(contributions_grants as float64) contributions_grants,
            safe_cast(
                contributions_grants_prior_year as float64
            ) contributions_grants_prior_year,
            safe_cast(program_service_revenue as float64) program_service_revenue,
            safe_cast(
                program_service_revenue_prior_year as float64
            ) program_service_revenue_prior_year,
            safe_cast(investment_income as float64) investment_income,
            safe_cast(
                investment_income_prior_year as float64
            ) investment_income_prior_year,
            safe_cast(other_revenue as float64) other_revenue,
            safe_cast(other_revenue_prior_year as float64) other_revenue_prior_year,
            safe_cast(total_revenue as float64) total_revenue,
            safe_cast(total_revenue_prior_year as float64) total_revenue_prior_year,
            safe_cast(grants_paid as float64) grants_paid,
            safe_cast(grants_paid_prior_year as float64) grants_paid_prior_year,
            safe_cast(benefits_paid_to_members as float64) benefits_paid_to_members,
            safe_cast(
                benefits_paid_to_members_prior_year as float64
            ) benefits_paid_to_members_prior_year,
            safe_cast(salaries_compensation as float64) salaries_compensation,
            safe_cast(
                salaries_compensation_prior_year as float64
            ) salaries_compensation_prior_year,
            safe_cast(
                professional_fundraising_fees as float64
            ) professional_fundraising_fees,
            safe_cast(
                professional_fundraising_fees_prior_year as float64
            ) professional_fundraising_fees_prior_year,
            safe_cast(total_fundraising_expenses as float64) total_fundraising_expenses,
            safe_cast(other_expenses as float64) other_expenses,
            safe_cast(other_expenses_prior_year as float64) other_expenses_prior_year,
            safe_cast(total_expenses as float64) total_expenses,
            safe_cast(total_expenses_prior_year as float64) total_expenses_prior_year,
            safe_cast(revenue_less_expenses as float64) revenue_less_expenses,
            safe_cast(
                revenue_less_expenses_prior_year as float64
            ) revenue_less_expenses_prior_year,
            safe_cast(total_assets_boy as float64) total_assets_boy,
            safe_cast(total_assets_eoy as float64) total_assets_eoy,
            safe_cast(total_liabilities_boy as float64) total_liabilities_boy,
            safe_cast(total_liabilities_eoy as float64) total_liabilities_eoy,
            safe_cast(net_assets_boy as float64) net_assets_boy,
            safe_cast(net_assets_eoy as float64) net_assets_eoy
        from {{ set_datalake_project("us_irs_form990_staging.return_financial") }} as t
    )
select *
from staged
qualify
    row_number() over (
        partition by ein, year, form_type order by return_timestamp desc, object_id desc
    )
    = 1
