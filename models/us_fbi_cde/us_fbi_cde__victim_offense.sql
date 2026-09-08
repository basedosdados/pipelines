{{
    config(
        alias="victim_offense",
        schema="us_fbi_cde",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1991, "end": 2030, "interval": 1},
        },
        cluster_by=["state_abbr"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(state_abbr as string) state_abbr,
    safe_cast(victim_id as string) victim_id,
    safe_cast(offense_id as string) offense_id
from {{ set_datalake_project("us_fbi_cde_staging.victim_offense") }} as t
