{{
    config(
        alias="house_member",
        schema="au_alexander_politicians",
        materialized="table",
    )
}}
select
    safe_cast(id_politician as string) id_politician,
    safe_cast(id_state as string) id_state,
    safe_cast(abbreviation_state as string) abbreviation_state,
    safe_cast(division as string) division,
    safe_cast(date_start as date) date_start,
    safe_cast(date_end as date) date_end,
    safe_cast(end_reason as string) end_reason,
    safe_cast(
        indicator_entered_at_by_election as string
    ) indicator_entered_at_by_election,
    safe_cast(indicator_changed_seat as string) indicator_changed_seat,
    safe_cast(comments as string) comments
from {{ set_datalake_project("au_alexander_politicians_staging.house_member") }} as t
