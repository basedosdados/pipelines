{{
    config(
        schema="us_irs_form990",
        alias="organization",
        materialized="incremental",
        partition_by={
            "field": "extraction_date",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

-- Atualizado em 2026-09-03
-- Each monthly BMF extract is a full snapshot; snapshots stack on
-- extraction_date and the incremental build appends new ones only.
select
    safe_cast(extraction_date as date) extraction_date,
    safe_cast(ein as string) ein,
    safe_cast(name as string) name,
    safe_cast(sort_name as string) sort_name,
    safe_cast(in_care_of_name as string) in_care_of_name,
    safe_cast(street as string) street,
    safe_cast(city as string) city,
    safe_cast(state as string) state,
    safe_cast(zip_code as string) zip_code,
    safe_cast(group_exemption_number as string) group_exemption_number,
    safe_cast(subsection_code as string) subsection_code,
    safe_cast(classification_code as string) classification_code,
    safe_cast(affiliation_code as string) affiliation_code,
    safe_cast(ruling_date as date) ruling_date,
    safe_cast(deductibility_code as string) deductibility_code,
    safe_cast(foundation_code as string) foundation_code,
    safe_cast(activity_code as string) activity_code,
    safe_cast(organization_code as string) organization_code,
    safe_cast(status_code as string) status_code,
    safe_cast(ntee_code as string) ntee_code,
    safe_cast(tax_period as string) tax_period,
    safe_cast(accounting_period_month as string) accounting_period_month,
    safe_cast(filing_requirement_code as string) filing_requirement_code,
    safe_cast(pf_filing_requirement_code as string) pf_filing_requirement_code,
    safe_cast(asset_code as string) asset_code,
    safe_cast(income_code as string) income_code,
    safe_cast(asset_amount as float64) asset_amount,
    safe_cast(income_amount as float64) income_amount,
    safe_cast(revenue_amount as float64) revenue_amount
from {{ set_datalake_project("us_irs_form990_staging.organization") }} as t
{% if is_incremental() %}
    where
        safe_cast(extraction_date as date)
        > (select max(extraction_date) from {{ this }})
{% endif %}
