{{
    config(
        alias="senator",
        schema="au_alexander_politicians",
        materialized="table",
    )
}}
select
    safe_cast(id_politician as string) id_politician,
    safe_cast(id_state as string) id_state,
    safe_cast(abbreviation_state as string) abbreviation_state,
    safe_cast(date_start as date) date_start,
    safe_cast(date_end as date) date_end,
    safe_cast(end_reason as string) end_reason,
    safe_cast(indicator_section_15_selection as string) indicator_section_15_selection,
    safe_cast(comments as string) comments
from {{ set_datalake_project("au_alexander_politicians_staging.senator") }} as t
