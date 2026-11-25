{{
    config(
        schema="world_cricsheet",
        alias="matches",
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
    safe_cast(end_date as date) end_date,
    safe_cast(match_type as string) match_type,
    safe_cast(team_type as string) team_type,
    safe_cast(gender as string) gender,
    safe_cast(event as string) event,
    safe_cast(match_number as string) match_number,
    safe_cast(balls_per_over as int64) balls_per_over,
    safe_cast(overs as int64) overs,
    safe_cast(venue as string) venue,
    safe_cast(city as string) city,
    safe_cast(team1 as string) team1,
    safe_cast(team2 as string) team2,
    safe_cast(toss_winner as string) toss_winner,
    safe_cast(toss_decision as string) toss_decision,
    safe_cast(player_of_match as string) player_of_match,
    safe_cast(winner as string) winner,
    safe_cast(outcome as string) outcome,
    safe_cast(winner_runs as int64) winner_runs,
    safe_cast(winner_wickets as int64) winner_wickets,
    safe_cast(winner_innings as string) winner_innings,
    safe_cast(method as string) method,
    safe_cast(target_runs as int64) target_runs,
    safe_cast(target_overs as int64) target_overs,
    safe_cast(neutral_venue as string) neutral_venue,
    safe_cast(umpire1 as string) umpire1,
    safe_cast(umpire2 as string) umpire2,
    safe_cast(tv_umpire as string) tv_umpire,
    safe_cast(reserve_umpire as string) reserve_umpire,
    safe_cast(match_referee as string) match_referee
from {{ set_datalake_project("world_cricsheet_staging.matches") }} as t
