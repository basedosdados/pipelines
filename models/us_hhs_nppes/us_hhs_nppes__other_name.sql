{{
    config(
        schema="us_hhs_nppes",
        alias="other_name",
        materialized="incremental",
        partition_by={
            "field": "extraction_date",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

-- Atualizado em 2026-09-02
select
    safe_cast(extraction_date as date) extraction_date,
    safe_cast(npi as string) npi,
    safe_cast(other_organization_name as string) other_organization_name,
    safe_cast(
        other_organization_name_type_code as string
    ) other_organization_name_type_code,
    safe_cast(created_date as date) created_date
from {{ set_datalake_project("us_hhs_nppes_staging.other_name") }} as t
{% if is_incremental() %}
    where
        safe_cast(extraction_date as date)
        > (select max(extraction_date) from {{ this }})
{% endif %}
