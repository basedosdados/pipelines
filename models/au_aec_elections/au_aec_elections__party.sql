{{
    config(
        schema="au_aec_elections",
        alias="party",
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
    safe_cast(party_abbreviation as string) party_abbreviation,
    safe_cast(registered_party_abbreviation as string) registered_party_abbreviation,
    safe_cast(party_name as string) party_name
from {{ set_datalake_project("au_aec_elections_staging.party") }} as t
