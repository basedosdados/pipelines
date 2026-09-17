{{
    config(
        schema="au_ato_abr",
        alias="dgr",
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
    safe_cast(dgr_status_from_date as date) dgr_status_from_date,
    safe_cast(dgr_name as string) dgr_name
from {{ set_datalake_project("au_ato_abr_staging.dgr") }} as t
{% if is_incremental() %}
    where
        safe_cast(extraction_date as date)
        > (select max(extraction_date) from {{ this }})
{% endif %}
