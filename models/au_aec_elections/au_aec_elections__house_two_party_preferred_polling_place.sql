{{
    config(
        schema="au_aec_elections",
        alias="house_two_party_preferred_polling_place",
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
    safe_cast(division_id as string) division_id,
    safe_cast(division_name as string) division_name,
    safe_cast(polling_place_id as string) polling_place_id,
    safe_cast(polling_place_name as string) polling_place_name,
    safe_cast(labor_votes as int64) labor_votes,
    safe_cast(labor_percentage as float64) labor_percentage,
    safe_cast(coalition_votes as int64) coalition_votes,
    safe_cast(coalition_percentage as float64) coalition_percentage,
    safe_cast(total_votes as int64) total_votes,
    safe_cast(swing as float64) swing
from
    {{
        set_datalake_project(
            "au_aec_elections_staging.house_two_party_preferred_polling_place"
        )
    }} as t
