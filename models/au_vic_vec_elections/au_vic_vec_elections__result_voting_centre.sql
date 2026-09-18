{{
    config(
        schema="au_vic_vec_elections",
        alias="result_voting_centre",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2002, "end": 2035, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(election_id as string) election_id,
    safe_cast(contest_id as string) contest_id,
    safe_cast(chamber as string) chamber,
    safe_cast(government_level as string) government_level,
    safe_cast(contest_type as string) contest_type,
    safe_cast(voting_system as string) voting_system,
    safe_cast(district_name as string) district_name,
    safe_cast(state_electoral_division_id as string) state_electoral_division_id,
    safe_cast(voting_centre_name as string) voting_centre_name,
    safe_cast(vote_type as string) vote_type,
    safe_cast(count_type as string) count_type,
    safe_cast(ballot_position as string) ballot_position,
    safe_cast(ballot_name as string) ballot_name,
    safe_cast(party_name as string) party_name,
    safe_cast(group_letter as string) group_letter,
    safe_cast(votes as int64) votes
from
    {{ set_datalake_project("au_vic_vec_elections_staging.result_voting_centre") }} as t
