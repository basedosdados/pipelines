{{
    config(
        schema="us_irs_form990",
        alias="revocation",
        materialized="table",
        partition_by={
            "field": "revocation_date",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

-- Atualizado em 2026-09-03
select
    safe_cast(ein as string) ein,
    safe_cast(legal_name as string) legal_name,
    safe_cast(doing_business_as_name as string) doing_business_as_name,
    safe_cast(address as string) address,
    safe_cast(city as string) city,
    safe_cast(state as string) state,
    safe_cast(zip_code as string) zip_code,
    safe_cast(country as string) country,
    safe_cast(exemption_type as string) exemption_type,
    safe_cast(revocation_date as date) revocation_date,
    safe_cast(revocation_posting_date as date) revocation_posting_date,
    safe_cast(exemption_reinstatement_date as date) exemption_reinstatement_date
from {{ set_datalake_project("us_irs_form990_staging.revocation") }} as t
