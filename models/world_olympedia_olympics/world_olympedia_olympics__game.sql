{{ config(alias="game", schema="world_olympedia_olympics") }}
select
    safe_cast(year as int64) year,
    safe_cast(edition as string) edition,
    safe_cast(edition_id as string) edition_id,
    safe_cast(city as string) city,
    safe_cast(country_flag_url as string) country_flag_url,
    safe_cast(country_noc as string) country_noc,
    safe_cast(start_date as date) start_date,
    safe_cast(end_date as date) end_date,
    safe_cast(competition_date as string) competition_date,
    safe_cast(isheld as string) is_held,
from {{ set_datalake_project("world_olympedia_olympics_staging.game") }} as t
