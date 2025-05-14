{{ config(alias="athlete_event_result", schema="world_olympedia_olympics") }}
select
    safe_cast(edition as string) edition,
    safe_cast(edition_id as string) edition_id,
    safe_cast(country_noc as string) country_noc,
    safe_cast(sport as string) sport,
    safe_cast(event as string) event,
    safe_cast(result_id as string) result_id,
    safe_cast(athlete as string) athlete,
    safe_cast(athlete_id as string) athlete_id,
    safe_cast(pos as string) position,
    safe_cast(medal as string) medal,
    safe_cast(isteamsport as bool) is_team_sport,
from
    {{ set_datalake_project("world_olympedia_olympics_staging.athlete_event_result") }}
    as t
