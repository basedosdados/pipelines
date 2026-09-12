{{
    config(
        schema="us_dot_bts_ontime",
        alias="airport",
        materialized="table",
    )
}}


select
    safe_cast(airport_id as string) airport_id,
    safe_cast(city_name as string) city_name,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(country_name as string) country_name,
    safe_cast(airport_name as string) airport_name,
    safe_cast(airport_description as string) airport_description
from {{ set_datalake_project("us_dot_bts_ontime_staging.airport") }} as t
