{{
    config(
        schema="au_ato_abr",
        alias="entity",
        materialized="incremental",
        partition_by={
            "field": "extraction_date",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

-- Atualizado em 2026-08-14
select
    safe_cast(extraction_date as date) extraction_date,
    safe_cast(abn as string) abn,
    safe_cast(abn_status as string) abn_status,
    safe_cast(abn_status_from_date as date) abn_status_from_date,
    safe_cast(entity_type as string) entity_type,
    safe_cast(entity_name as string) entity_name,
    safe_cast(asic_number as string) asic_number,
    safe_cast(asic_number_type as string) asic_number_type,
    safe_cast(gst_status as string) gst_status,
    safe_cast(gst_status_from_date as date) gst_status_from_date,
    safe_cast(state_code as string) state_code,
    safe_cast(postcode as string) postcode,
    safe_cast(record_last_updated_date as date) record_last_updated_date,
    safe_cast(replaced as string) replaced
from {{ set_datalake_project("au_ato_abr_staging.entity") }} as t
{% if is_incremental() %}
    where
        safe_cast(extraction_date as date)
        > (select max(extraction_date) from {{ this }})
{% endif %}
