{{ config(alias="result", schema="world_olympedia_olympics") }}
select
    safe_cast(result_id as string) result_id,
    safe_cast(event_title as string) event_title,
    safe_cast(edition as string) edition,
    safe_cast(edition_id as string) edition_id,
    safe_cast(sport as string) sport,
    safe_cast(result_date as string) result_date,
    safe_cast(result_location as string) result_location,
    safe_cast(result_participants as string) result_participants,
    safe_cast(result_format as string) result_format,
    safe_cast(result_detail as string) result_detail,
    safe_cast(result_description as string) result_description,
from {{ set_datalake_project("world_olympedia_olympics_staging.result") }} as t
