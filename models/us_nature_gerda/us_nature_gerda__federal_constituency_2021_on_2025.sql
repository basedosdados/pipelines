{{
    config(
        schema="us_nature_gerda",
        alias="federal_constituency_2021_on_2025",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1945, "end": 2031, "interval": 1},
        },
        cluster_by=["id_constituency", "party"],
    )
}}

select
    safe_cast(year as int64) year,
    safe_cast(id_constituency as string) id_constituency,
    safe_cast(constituency_name as string) constituency_name,
    safe_cast(id_state as string) id_state,
    safe_cast(ballot as string) ballot,
    safe_cast(boundary_change as string) boundary_change,
    safe_cast(eligible_voters as int64) eligible_voters,
    safe_cast(valid_votes as int64) valid_votes,
    safe_cast(party as string) party,
    safe_cast(vote_share as float64) vote_share
from
    {{
        set_datalake_project(
            "us_nature_gerda_staging.federal_constituency_2021_on_2025"
        )
    }} as t
