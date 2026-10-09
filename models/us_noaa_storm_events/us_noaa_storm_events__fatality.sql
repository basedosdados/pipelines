{{
    config(
        schema="us_noaa_storm_events",
        alias="fatality",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1950, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(event_id as string) event_id,
    safe_cast(fatality_id as string) fatality_id,
    safe_cast(fatality_datetime as datetime) fatality_datetime,
    safe_cast(fatality_type as string) fatality_type,
    safe_cast(age as int64) age,
    safe_cast(sex as string) sex,
    safe_cast(location as string) location
from {{ set_datalake_project("us_noaa_storm_events_staging.fatality") }} as t
