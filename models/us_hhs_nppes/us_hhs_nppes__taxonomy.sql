{{
    config(
        schema="us_hhs_nppes",
        alias="taxonomy",
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
    safe_cast(taxonomy_sequence as string) taxonomy_sequence,
    safe_cast(taxonomy_code as string) taxonomy_code,
    safe_cast(is_primary_taxonomy as string) is_primary_taxonomy,
    safe_cast(license_number as string) license_number,
    safe_cast(license_state_code as string) license_state_code,
    safe_cast(taxonomy_group_code as string) taxonomy_group_code,
    safe_cast(taxonomy_group_name as string) taxonomy_group_name
from {{ set_datalake_project("us_hhs_nppes_staging.taxonomy") }} as t
{% if is_incremental() %}
    where
        safe_cast(extraction_date as date)
        > (select max(extraction_date) from {{ this }})
{% endif %}
