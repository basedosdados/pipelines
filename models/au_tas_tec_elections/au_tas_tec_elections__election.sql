{{
    config(
        schema="au_tas_tec_elections",
        alias="election",
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
    safe_cast(election_name as string) election_name,
    safe_cast(chamber as string) chamber,
    safe_cast(election_type as string) election_type,
    safe_cast(election_date as date) election_date,
    safe_cast(seats_per_division as int64) seats_per_division,
    safe_cast(results_index_url as string) results_index_url
from {{ set_datalake_project("au_tas_tec_elections_staging.election") }} as t
