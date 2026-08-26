{{
    config(
        schema="au_aec_elections",
        alias="division_summary",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1998, "end": 2035, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(election_id as string) election_id,
    safe_cast(chamber as string) chamber,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(division_id as string) division_id,
    safe_cast(division_name as string) division_name,
    safe_cast(enrolment as int64) enrolment,
    safe_cast(turnout as int64) turnout,
    safe_cast(turnout_percentage as float64) turnout_percentage,
    safe_cast(turnout_swing as float64) turnout_swing,
    safe_cast(ordinary_votes as int64) ordinary_votes,
    safe_cast(absent_votes as int64) absent_votes,
    safe_cast(provisional_votes as int64) provisional_votes,
    safe_cast(pre_poll_votes as int64) pre_poll_votes,
    safe_cast(postal_votes as int64) postal_votes,
    safe_cast(formal_votes as int64) formal_votes,
    safe_cast(informal_votes as int64) informal_votes,
    safe_cast(informal_percentage as float64) informal_percentage,
    safe_cast(informal_swing as float64) informal_swing,
    safe_cast(total_votes as int64) total_votes
from {{ set_datalake_project("au_aec_elections_staging.division_summary") }} as t
