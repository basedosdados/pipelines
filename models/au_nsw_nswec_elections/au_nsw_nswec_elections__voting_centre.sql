{{
    config(
        schema="au_nsw_nswec_elections",
        alias="voting_centre",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2011, "end": 2035, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(election_id as string) election_id,
    safe_cast(state_electoral_division_id as string) state_electoral_division_id,
    safe_cast(district_name as string) district_name,
    safe_cast(voting_centre_name as string) voting_centre_name,
    safe_cast(vote_type_code as string) vote_type_code,
    safe_cast(vote_sub_type as string) vote_sub_type,
    safe_cast(premises_name as string) premises_name,
    safe_cast(address as string) address,
    safe_cast(locality as string) locality,
    safe_cast(postcode as string) postcode,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude,
    safe_cast(is_in_results as string) is_in_results
from {{ set_datalake_project("au_nsw_nswec_elections_staging.voting_centre") }} as t
