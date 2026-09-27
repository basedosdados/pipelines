{{
    config(
        schema="au_sa_ecsa_elections",
        alias="voting_centre",
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
    safe_cast(state_electoral_division_id as string) state_electoral_division_id,
    safe_cast(district_name as string) district_name,
    safe_cast(voting_centre_name as string) voting_centre_name,
    safe_cast(voting_centre_type as string) voting_centre_type
from {{ set_datalake_project("au_sa_ecsa_elections_staging.voting_centre") }} as t
