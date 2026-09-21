{{
    config(
        schema="au_aec_elections",
        alias="election",
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
    safe_cast(election_name as string) election_name,
    safe_cast(election_type as string) election_type,
    safe_cast(division_name as string) division_name
from {{ set_datalake_project("au_aec_elections_staging.election") }} as t
