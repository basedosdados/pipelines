{{
    config(
        schema="au_aec_elections",
        alias="polling_place",
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
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(election_id as string) election_id,
    safe_cast(division_id as string) division_id,
    safe_cast(division_name as string) division_name,
    safe_cast(polling_place_id as string) polling_place_id,
    safe_cast(polling_place_type_id as string) polling_place_type_id,
    safe_cast(polling_place_name as string) polling_place_name,
    safe_cast(premises_state_abbreviation as string) premises_state_abbreviation,
    safe_cast(premises_name as string) premises_name,
    safe_cast(premises_address_1 as string) premises_address_1,
    safe_cast(premises_address_2 as string) premises_address_2,
    safe_cast(premises_address_3 as string) premises_address_3,
    safe_cast(premises_suburb as string) premises_suburb,
    safe_cast(premises_postcode as string) premises_postcode,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude
from {{ set_datalake_project("au_aec_elections_staging.polling_place") }} as t
