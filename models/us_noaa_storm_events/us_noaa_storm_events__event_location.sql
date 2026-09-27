{{
    config(
        schema="us_noaa_storm_events",
        alias="event_location",
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
    safe_cast(episode_id as string) episode_id,
    safe_cast(location_index as string) location_index,
    safe_cast(location_range as float64) location_range,
    safe_cast(location_azimuth as string) location_azimuth,
    safe_cast(location_name as string) location_name,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude
from {{ set_datalake_project("us_noaa_storm_events_staging.event_location") }} as t
