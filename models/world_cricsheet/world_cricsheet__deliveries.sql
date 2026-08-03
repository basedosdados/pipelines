{{
    config(
        schema="world_cricsheet",
        alias="deliveries",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1770, "end": 2035, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(match_id as string) match_id,
    safe_cast(season as string) season,
    safe_cast(start_date as date) start_date,
    safe_cast(venue as string) venue,
    safe_cast(innings as string) innings,
    safe_cast(ball as string) ball,
    safe_cast(actual_delivery as string) actual_delivery,
    safe_cast(batting_team as string) batting_team,
    safe_cast(bowling_team as string) bowling_team,
    safe_cast(striker as string) striker,
    safe_cast(non_striker as string) non_striker,
    safe_cast(bowler as string) bowler,
    safe_cast(runs_off_bat as int64) runs_off_bat,
    safe_cast(extras as int64) extras,
    safe_cast(wides as int64) wides,
    safe_cast(noballs as int64) noballs,
    safe_cast(byes as int64) byes,
    safe_cast(legbyes as int64) legbyes,
    safe_cast(penalty as int64) penalty,
    safe_cast(non_boundary as string) non_boundary,
    safe_cast(wicket_type as string) wicket_type,
    safe_cast(player_dismissed as string) player_dismissed,
    safe_cast(other_wicket_type as string) other_wicket_type,
    safe_cast(other_player_dismissed as string) other_player_dismissed,
    safe_cast(fielder_1 as string) fielder_1,
    safe_cast(fielder_2 as string) fielder_2,
    safe_cast(fielder_3 as string) fielder_3
from {{ set_datalake_project("world_cricsheet_staging.deliveries") }} as t
