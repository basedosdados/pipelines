{{
    config(
        schema="au_vic_vec_elections",
        alias="enrolment_turnout",
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
    safe_cast(enrolment as int64) enrolment,
    safe_cast(votes_formal as int64) votes_formal,
    safe_cast(votes_informal as int64) votes_informal,
    safe_cast(votes_total as int64) votes_total,
    safe_cast(percentage_informal as float64) percentage_informal,
    safe_cast(percentage_turnout as float64) percentage_turnout,
    safe_cast(quota as int64) quota,
    safe_cast(seats_to_elect as int64) seats_to_elect
from {{ set_datalake_project("au_vic_vec_elections_staging.enrolment_turnout") }} as t
