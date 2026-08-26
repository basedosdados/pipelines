{{
    config(
        schema="au_aec_elections",
        alias="senate_candidate",
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
    safe_cast(candidate_id as string) candidate_id,
    safe_cast(surname as string) surname,
    safe_cast(given_name as string) given_name,
    safe_cast(party_abbreviation as string) party_abbreviation,
    safe_cast(party_name as string) party_name,
    safe_cast(elected as string) elected,
    safe_cast(historic_elected as string) historic_elected,
    safe_cast(sitting_member as string) sitting_member,
    safe_cast(elected_order as string) elected_order
from {{ set_datalake_project("au_aec_elections_staging.senate_candidate") }} as t
