{{
    config(
        schema="us_nature_gerda",
        alias="state_municipality_harmonized_2021",
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
    safe_cast(eligible_voters as int64) eligible_voters,
    safe_cast(voters as int64) voters,
    safe_cast(valid_votes as int64) valid_votes,
    safe_cast(invalid_votes as int64) invalid_votes,
    safe_cast(turnout as float64) turnout,
    safe_cast(flag_harm_turnout_above_1 as string) flag_harm_turnout_above_1,
    safe_cast(flag_unsuccessful_naive_merge as string) flag_unsuccessful_naive_merge,
    safe_cast(flag_other_party_residual as string) flag_other_party_residual,
    safe_cast(party as string) party,
    safe_cast(vote_share as float64) vote_share
from
    {{
        set_datalake_project(
            "us_nature_gerda_staging.state_municipality_harmonized_2021"
        )
    }} as t
