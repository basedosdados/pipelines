{{
    config(
        schema="au_ecq_elections",
        alias="voting_centre",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2035, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(election_id as string) election_id,
    safe_cast(voting_centre_id as string) voting_centre_id,
    safe_cast(state_electoral_division_id as string) state_electoral_division_id,
    safe_cast(district_name as string) district_name,
    safe_cast(voting_centre_name as string) voting_centre_name,
    safe_cast(building_name as string) building_name,
    safe_cast(street_number as string) street_number,
    safe_cast(street_name as string) street_name,
    safe_cast(locality as string) locality,
    safe_cast(postcode as string) postcode,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude,
    safe_cast(joint_type as string) joint_type,
    safe_cast(is_abolished as string) is_abolished
from {{ set_datalake_project("au_ecq_elections_staging.voting_centre") }} as t
