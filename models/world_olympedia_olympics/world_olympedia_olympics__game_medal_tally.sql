{{ config(alias="game_medal_tally", schema="world_olympedia_olympics") }}
select
    safe_cast(year as int64) year,
    safe_cast(edition as string) edition,
    safe_cast(edition_id as string) edition_id,
    safe_cast(country as string) country,
    safe_cast(country_noc as string) country_noc,
    safe_cast(gold as int64) gold,
    safe_cast(silver as int64) silver,
    safe_cast(bronze as int64) bronze,
    safe_cast(total as int64) total,
from
    {{ set_datalake_project("world_olympedia_olympics_staging.game_medal_tally") }} as t
