{{
    config(
        schema="world_cricsheet",
        alias="match_players",
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
    safe_cast(team as string) team_name,
    safe_cast(player as string) player_name,
    safe_cast(player_identifier as string) person_id
from {{ set_datalake_project("world_cricsheet_staging.match_players") }} as t
