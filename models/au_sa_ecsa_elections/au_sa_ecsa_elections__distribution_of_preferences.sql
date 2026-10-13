{{
    config(
        schema="au_sa_ecsa_elections",
        alias="distribution_of_preferences",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2015, "end": 2035, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(election_id as string) election_id,
    safe_cast(contest_id as string) contest_id,
    safe_cast(state_electoral_division_id as string) state_electoral_division_id,
    safe_cast(chamber as string) chamber,
    safe_cast(government_level as string) government_level,
    safe_cast(contest_type as string) contest_type,
    safe_cast(voting_system as string) voting_system,
    safe_cast(district_name as string) district_name,
    safe_cast(round_number as string) round_number,
    safe_cast(round_type as string) round_type,
    safe_cast(excluded_ballot_name as string) excluded_ballot_name,
    safe_cast(votes_excluded as int64) votes_excluded,
    safe_cast(ballot_order_number as string) ballot_order_number,
    safe_cast(ballot_name as string) ballot_name,
    safe_cast(party_code as string) party_code,
    safe_cast(party_name as string) party_name,
    safe_cast(votes_transferred as int64) votes_transferred,
    safe_cast(votes_progressive_total as int64) votes_progressive_total,
    safe_cast(is_excluded as string) is_excluded,
    safe_cast(is_elected as string) is_elected
from
    {{
        set_datalake_project(
            "au_sa_ecsa_elections_staging.distribution_of_preferences"
        )
    }} as t
