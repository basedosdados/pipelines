{{
    config(
        schema="au_sa_ecsa_elections",
        alias="enrolment_turnout",
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
    safe_cast(enrolment as int64) enrolment,
    safe_cast(candidates_count as int64) candidates_count,
    safe_cast(votes_formal as int64) votes_formal,
    safe_cast(votes_informal as int64) votes_informal,
    safe_cast(votes_total as int64) votes_total,
    safe_cast(percentage_informal as float64) percentage_informal,
    safe_cast(percentage_roll_counted as float64) percentage_roll_counted,
    safe_cast(polling_places_counted as int64) polling_places_counted,
    safe_cast(polling_places_total as int64) polling_places_total
from {{ set_datalake_project("au_sa_ecsa_elections_staging.enrolment_turnout") }} as t
