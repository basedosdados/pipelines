{{
    config(
        schema="au_abs_population",
        alias="national_state",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1981, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(quarter as int64) quarter,
    safe_cast(geography_level as string) geography_level,
    safe_cast(state_id as string) state_id,
    safe_cast(region_name as string) region_name,
    safe_cast(sex as string) sex,
    safe_cast(measure as string) measure,
    safe_cast(unit as string) unit,
    safe_cast(series_id as string) series_id,
    safe_cast(value as float64) value
from {{ set_datalake_project("au_abs_population_staging.national_state") }} as t
