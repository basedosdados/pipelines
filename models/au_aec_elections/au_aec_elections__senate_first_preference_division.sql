{{
    config(
        schema="au_aec_elections",
        alias="senate_first_preference_division",
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
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(election_id as string) election_id,
    safe_cast(division_id as string) division_id,
    safe_cast(division_name as string) division_name,
    safe_cast(group_abbreviation as string) group_abbreviation,
    safe_cast(ballot_position as string) ballot_position,
    safe_cast(candidate_id as string) candidate_id,
    safe_cast(candidate_details as string) candidate_details,
    safe_cast(party_abbreviation as string) party_abbreviation,
    safe_cast(party_name as string) party_name,
    safe_cast(elected as string) elected,
    safe_cast(historic_elected as string) historic_elected,
    safe_cast(ordinary_votes as int64) ordinary_votes,
    safe_cast(absent_votes as int64) absent_votes,
    safe_cast(provisional_votes as int64) provisional_votes,
    safe_cast(pre_poll_votes as int64) pre_poll_votes,
    safe_cast(postal_votes as int64) postal_votes,
    safe_cast(total_votes as int64) total_votes
from
    {{
        set_datalake_project(
            "au_aec_elections_staging.senate_first_preference_division"
        )
    }} as t
