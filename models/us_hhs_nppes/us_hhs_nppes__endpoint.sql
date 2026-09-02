{{
    config(
        schema="us_hhs_nppes",
        alias="endpoint",
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
    safe_cast(endpoint_type as string) endpoint_type,
    safe_cast(endpoint as string) endpoint,
    safe_cast(endpoint_description as string) endpoint_description,
    safe_cast(use_code as string) use_code,
    safe_cast(other_use_description as string) other_use_description,
    safe_cast(content_type as string) content_type,
    safe_cast(other_content_description as string) other_content_description,
    safe_cast(affiliation as string) affiliation,
    safe_cast(
        affiliation_legal_business_name as string
    ) affiliation_legal_business_name,
    safe_cast(affiliation_address_line_1 as string) affiliation_address_line_1,
    safe_cast(affiliation_address_line_2 as string) affiliation_address_line_2,
    safe_cast(affiliation_address_city as string) affiliation_address_city,
    safe_cast(affiliation_address_state as string) affiliation_address_state,
    safe_cast(
        affiliation_address_postal_code as string
    ) affiliation_address_postal_code,
    safe_cast(
        affiliation_address_country_code as string
    ) affiliation_address_country_code
from {{ set_datalake_project("us_hhs_nppes_staging.endpoint") }} as t
{% if is_incremental() %}
    where
        safe_cast(extraction_date as date)
        > (select max(extraction_date) from {{ this }})
{% endif %}
