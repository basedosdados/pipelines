{{
    config(
        schema="us_hhs_nppes",
        alias="other_identifier",
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
    safe_cast(identifier_sequence as string) identifier_sequence,
    safe_cast(other_identifier as string) other_identifier,
    safe_cast(other_identifier_type_code as string) other_identifier_type_code,
    safe_cast(other_identifier_state_code as string) other_identifier_state_code,
    safe_cast(other_identifier_issuer as string) other_identifier_issuer
from {{ set_datalake_project("us_hhs_nppes_staging.other_identifier") }} as t
{% if is_incremental() %}
    where
        safe_cast(extraction_date as date)
        > (select max(extraction_date) from {{ this }})
{% endif %}
