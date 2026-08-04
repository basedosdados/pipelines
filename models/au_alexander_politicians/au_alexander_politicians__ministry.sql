{{
    config(
        alias="ministry",
        schema="au_alexander_politicians",
        materialized="table",
    )
}}
select
    safe_cast(id_politician as string) id_politician,
    safe_cast(ministry as string) ministry,
    safe_cast(ministry_number as string) ministry_number,
    safe_cast(ministry_party as string) ministry_party,
    safe_cast(ministry_title as string) ministry_title,
    safe_cast(display_name as string) display_name,
    safe_cast(date_start as date) date_start,
    safe_cast(date_end as date) date_end,
    safe_cast(
        indicator_assistant_or_secretary as string
    ) indicator_assistant_or_secretary,
    safe_cast(comments as string) comments
from {{ set_datalake_project("au_alexander_politicians_staging.ministry") }} as t
