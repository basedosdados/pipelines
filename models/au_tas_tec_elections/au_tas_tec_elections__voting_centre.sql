{{
    config(
        schema="au_tas_tec_elections",
        alias="voting_centre",
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
    safe_cast(voting_centre_name as string) voting_centre_name,
    safe_cast(locality as string) locality,
    safe_cast(premise_name as string) premise_name,
    safe_cast(premise_address as string) premise_address,
    safe_cast(postcode as string) postcode,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(location_within_premise as string) location_within_premise,
    safe_cast(disabled_access as string) disabled_access,
    safe_cast(district_name as string) district_name
from {{ set_datalake_project("au_tas_tec_elections_staging.voting_centre") }} as t
