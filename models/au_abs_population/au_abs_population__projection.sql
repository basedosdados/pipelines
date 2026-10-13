{{
    config(
        schema="au_abs_population",
        alias="projection",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2022, "end": 2076, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(projection_base_year as int64) projection_base_year,
    safe_cast(series as string) series,
    safe_cast(geography_level as string) geography_level,
    safe_cast(state_id as string) state_id,
    safe_cast(region_name as string) region_name,
    safe_cast(sex as string) sex,
    safe_cast(age as string) age,
    safe_cast(series_id as string) series_id,
    safe_cast(projected_population as int64) projected_population
from {{ set_datalake_project("au_abs_population_staging.projection") }} as t
