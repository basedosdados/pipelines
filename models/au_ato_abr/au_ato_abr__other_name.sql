{{
    config(
        schema="au_ato_abr",
        alias="other_name",
        materialized="incremental",
        partition_by={
            "field": "extraction_date",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

-- Atualizado em 2026-08-14
-- distinct: the source occasionally repeats the identical (name_type, name) for
-- an ABN within one snapshot; those rows carry no extra information.
select distinct
    safe_cast(extraction_date as date) extraction_date,
    safe_cast(abn as string) abn,
    safe_cast(name_type as string) name_type,
    safe_cast(name as string) name
from {{ set_datalake_project("au_ato_abr_staging.other_name") }} as t
{% if is_incremental() %}
    where
        safe_cast(extraction_date as date)
        > (select max(extraction_date) from {{ this }})
{% endif %}
