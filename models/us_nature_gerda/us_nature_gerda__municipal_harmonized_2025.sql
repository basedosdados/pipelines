{{
    config(
        schema="us_nature_gerda",
        alias="municipal_harmonized_2025",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1945, "end": 2031, "interval": 1},
        },
        cluster_by=["id_municipality", "party"],
    )
}}

select
    safe_cast(year as int64) year,
    safe_cast(election_date as date) election_date,
    safe_cast(id_municipality as string) id_municipality,
    safe_cast(id_county as string) id_county,
    safe_cast(id_state as string) id_state,
    safe_cast(election_type as string) election_type,
    safe_cast(eligible_voters as int64) eligible_voters,
    safe_cast(voters as int64) voters,
    safe_cast(valid_votes as int64) valid_votes,
    safe_cast(turnout as float64) turnout,
    safe_cast(flag_unsuccessful_naive_merge as string) flag_unsuccessful_naive_merge,
    safe_cast(party as string) party,
    safe_cast(vote_share as float64) vote_share,
    safe_cast(seats as int64) seats
from
    {{ set_datalake_project("us_nature_gerda_staging.municipal_harmonized_2025") }} as t
