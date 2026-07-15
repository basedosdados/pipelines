{{
    config(
        schema="us_medsl_elections",
        alias="precinct",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2016, "end": 2029, "interval": 1},
        },
        cluster_by=["id_state", "office"],
    )
}}

select
    safe_cast(year as int64) year,
    safe_cast(nullif(date, '') as date) date,
    safe_cast(nullif(stage, '') as string) stage,
    safe_cast(id_state as string) id_state,
    safe_cast(nullif(id_county, '') as string) id_county,
    safe_cast(nullif(id_jurisdiction, '') as string) id_jurisdiction,
    safe_cast(nullif(jurisdiction_name, '') as string) jurisdiction_name,
    safe_cast(nullif(precinct, '') as string) precinct,
    safe_cast(nullif(office, '') as string) office,
    safe_cast(nullif(office_category, '') as string) office_category,
    safe_cast(nullif(district, '') as string) district,
    safe_cast(magnitude as int64) magnitude,
    safe_cast(nullif(candidate, '') as string) candidate,
    safe_cast(nullif(party_detailed, '') as string) party_detailed,
    safe_cast(nullif(party_simplified, '') as string) party_simplified,
    safe_cast(nullif(indicator_special, '') as bool) indicator_special,
    safe_cast(nullif(indicator_writein, '') as bool) indicator_writein,
    safe_cast(nullif(mode, '') as string) mode,
    safe_cast(votes as int64) votes,
    safe_cast(nullif(indicator_readme_check, '') as bool) indicator_readme_check
from {{ set_datalake_project("us_medsl_elections_staging.precinct") }} as t
