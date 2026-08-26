{{
    config(
        schema="au_aec_elections",
        alias="house_first_preference_division",
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
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(division_id as string) division_id,
    safe_cast(division_name as string) division_name,
    safe_cast(candidate_id as string) candidate_id,
    safe_cast(surname as string) surname,
    safe_cast(given_name as string) given_name,
    safe_cast(ballot_position as string) ballot_position,
    safe_cast(party_abbreviation as string) party_abbreviation,
    safe_cast(party_name as string) party_name,
    safe_cast(elected as string) elected,
    safe_cast(historic_elected as string) historic_elected,
    safe_cast(sitting_member as string) sitting_member,
    safe_cast(ordinary_votes as int64) ordinary_votes,
    safe_cast(absent_votes as int64) absent_votes,
    safe_cast(provisional_votes as int64) provisional_votes,
    safe_cast(pre_poll_votes as int64) pre_poll_votes,
    safe_cast(postal_votes as int64) postal_votes,
    safe_cast(total_votes as int64) total_votes,
    safe_cast(swing as float64) swing
from
    {{
        set_datalake_project(
            "au_aec_elections_staging.house_first_preference_division"
        )
    }} as t
