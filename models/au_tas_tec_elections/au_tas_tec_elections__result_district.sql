{{
    config(
        schema="au_tas_tec_elections",
        alias="result_district",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2017, "end": 2035, "interval": 1},
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
    safe_cast(
        commonwealth_electoral_division_id as string
    ) commonwealth_electoral_division_id,
    safe_cast(state_electoral_division_id as string) state_electoral_division_id,
    safe_cast(count_type as string) count_type,
    safe_cast(ballot_name as string) ballot_name,
    safe_cast(party_name as string) party_name,
    safe_cast(votes as int64) votes,
    safe_cast(percentage_formal_votes as float64) percentage_formal_votes,
    safe_cast(quotas as float64) quotas,
    safe_cast(candidate_status as string) candidate_status
from {{ set_datalake_project("au_tas_tec_elections_staging.result_district") }} as t
