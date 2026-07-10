{{
    config(
        schema="us_medsl_elections",
        alias="district",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1976, "end": 2029, "interval": 1},
        },
        cluster_by=["id_state"],
    )
}}

select
    safe_cast(year as int64) year,
    safe_cast(id_state as string) id_state,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(state_census_code as string) state_census_code,
    safe_cast(state_icpsr_code as string) state_icpsr_code,
    safe_cast(district as string) district,
    safe_cast(nullif(stage, '') as string) stage,
    safe_cast(nullif(indicator_runoff, '') as bool) indicator_runoff,
    safe_cast(nullif(indicator_special, '') as bool) indicator_special,
    safe_cast(candidate as string) candidate,
    safe_cast(party_detailed as string) party_detailed,
    safe_cast(nullif(party_simplified, '') as string) party_simplified,
    safe_cast(nullif(indicator_writein, '') as bool) indicator_writein,
    safe_cast(nullif(mode, '') as string) mode,
    safe_cast(votes as int64) votes,
    safe_cast(total_votes as int64) total_votes,
    safe_cast(nullif(indicator_unofficial, '') as bool) indicator_unofficial,
    safe_cast(nullif(indicator_fusion_ticket, '') as bool) indicator_fusion_ticket,
    safe_cast(version as string) version
from {{ set_datalake_project("us_medsl_elections_staging.district") }} as t
