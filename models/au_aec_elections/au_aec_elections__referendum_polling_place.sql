{{
    config(
        schema="au_aec_elections",
        alias="referendum_polling_place",
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
    safe_cast(question_number as string) question_number,
    safe_cast(division_id as string) division_id,
    safe_cast(division_name as string) division_name,
    safe_cast(polling_place_id as string) polling_place_id,
    safe_cast(polling_place_name as string) polling_place_name,
    safe_cast(yes_votes as int64) yes_votes,
    safe_cast(yes_percentage as float64) yes_percentage,
    safe_cast(no_votes as int64) no_votes,
    safe_cast(no_percentage as float64) no_percentage,
    safe_cast(formal_votes as int64) formal_votes,
    safe_cast(formal_percentage as float64) formal_percentage,
    safe_cast(informal_votes as int64) informal_votes,
    safe_cast(informal_percentage as float64) informal_percentage,
    safe_cast(total_votes as int64) total_votes
from
    {{ set_datalake_project("au_aec_elections_staging.referendum_polling_place") }} as t
