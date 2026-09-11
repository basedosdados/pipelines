{{
    config(
        schema="au_nsw_nswec_elections",
        alias="ballot_preference",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2011, "end": 2035, "interval": 1},
        },
        cluster_by=["contest_id", "voting_centre_name"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(election_id as string) election_id,
    safe_cast(contest_id as string) contest_id,
    safe_cast(state_electoral_division_id as string) state_electoral_division_id,
    safe_cast(chamber as string) chamber,
    safe_cast(district_name as string) district_name,
    safe_cast(voting_centre_name as string) voting_centre_name,
    safe_cast(ballot_paper_id as string) ballot_paper_id,
    safe_cast(formality as string) formality,
    safe_cast(ballot_name as string) ballot_name,
    safe_cast(party_code as string) party_code,
    safe_cast(preference_number as string) preference_number,
    safe_cast(preference_counted_number as string) preference_counted_number
from {{ set_datalake_project("au_nsw_nswec_elections_staging.ballot_preference") }} as t
