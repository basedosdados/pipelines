{{
    config(
        schema="us_fed_fred",
        alias="series",
        materialized="table",
    )
}}


select
    safe_cast(series_id as string) series_id,
    safe_cast(title as string) title,
    safe_cast(units as string) units,
    safe_cast(units_short as string) units_short,
    safe_cast(frequency as string) frequency,
    safe_cast(frequency_short as string) frequency_short,
    safe_cast(seasonal_adjustment as string) seasonal_adjustment,
    safe_cast(seasonal_adjustment_short as string) seasonal_adjustment_short,
    safe_cast(observation_start as date) observation_start,
    safe_cast(observation_end as date) observation_end,
    safe_cast(last_updated as string) last_updated,
    safe_cast(source_name as string) source_name,
    safe_cast(release_name as string) release_name,
    safe_cast(notes as string) notes
from {{ set_datalake_project("us_fed_fred_staging.series") }} as t
