{{
    config(
        alias="jurisdiction",
        schema="us_stanford_open_policing",
        materialized="table",
    )
}}
select
    safe_cast(jurisdiction_id as string) jurisdiction_id,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(state_id as string) state_id,
    safe_cast(jurisdiction_name as string) jurisdiction_name,
    safe_cast(jurisdiction_type as string) jurisdiction_type,
    safe_cast(release_date as date) release_date,
    safe_cast(start_date as date) start_date,
    safe_cast(end_date as date) end_date,
    safe_cast(number_stops as int64) number_stops,
    safe_cast(source_url as string) source_url
from {{ set_datalake_project("us_stanford_open_policing_staging.jurisdiction") }} as t
