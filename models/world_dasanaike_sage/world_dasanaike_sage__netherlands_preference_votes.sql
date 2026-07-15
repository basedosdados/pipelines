{{
    config(
        schema="world_dasanaike_sage",
        alias="netherlands_preference_votes",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2017, "end": 2028, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(gemeente_code as string) gemeente_code,
    safe_cast(stembureau_id as string) stembureau_id,
    safe_cast(party_id as string) party_id,
    safe_cast(party as string) party,
    safe_cast(candidate_id as string) candidate_id,
    safe_cast(candidate as string) candidate,
    safe_cast(votes as int64) votes
from
    {{
        set_datalake_project(
            "world_dasanaike_sage_staging.netherlands_preference_votes"
        )
    }} as t
