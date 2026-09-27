{{
    config(
        schema="us_hhs_nppes",
        alias="practice_location",
        materialized="incremental",
        partition_by={
            "field": "extraction_date",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

-- Atualizado em 2026-09-02
-- distinct: the source repeats byte-identical rows for an NPI
-- within one snapshot; the repeats carry no extra information.
select distinct
    safe_cast(extraction_date as date) extraction_date,
    safe_cast(npi as string) npi,
    safe_cast(address_line_1 as string) address_line_1,
    safe_cast(address_line_2 as string) address_line_2,
    safe_cast(address_city as string) address_city,
    safe_cast(address_state as string) address_state,
    safe_cast(address_postal_code as string) address_postal_code,
    safe_cast(address_country_code as string) address_country_code,
    safe_cast(telephone_number as string) telephone_number,
    safe_cast(telephone_extension as string) telephone_extension,
    safe_cast(fax_number as string) fax_number
from {{ set_datalake_project("us_hhs_nppes_staging.practice_location") }} as t
{% if is_incremental() %}
    where
        safe_cast(extraction_date as date)
        > (select max(extraction_date) from {{ this }})
{% endif %}
