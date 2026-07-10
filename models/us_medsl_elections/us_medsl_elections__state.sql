{{
    config(
        schema="us_medsl_elections",
        alias="state",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1976, "end": 2029, "interval": 1},
        },
        cluster_by=["id_state", "office"],
    )
}}

select
    safe_cast(year as int64) year,
    safe_cast(nullif(stage, '') as string) stage,
    safe_cast(id_state as string) id_state,
    safe_cast(office as string) office,
    safe_cast(nullif(district, '') as string) district,
    safe_cast(nullif(indicator_special, '') as bool) indicator_special,
    safe_cast(candidate as string) candidate,
    safe_cast(party_detailed as string) party_detailed,
    safe_cast(nullif(party_simplified, '') as string) party_simplified,
    safe_cast(nullif(indicator_writein, '') as bool) indicator_writein,
    safe_cast(nullif(mode, '') as string) mode,
    safe_cast(votes as int64) votes,
    safe_cast(total_votes as int64) total_votes,
    safe_cast(nullif(indicator_unofficial, '') as bool) indicator_unofficial,
    safe_cast(version as string) version
from {{ set_datalake_project("us_medsl_elections_staging.state") }} as t
