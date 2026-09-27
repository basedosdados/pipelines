{{
    config(
        schema="au_abs_population",
        alias="erp_age_sex",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1971, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(geography_level as string) geography_level,
    safe_cast(state_id as string) state_id,
    safe_cast(region_name as string) region_name,
    safe_cast(sex as string) sex,
    safe_cast(age as string) age,
    safe_cast(series_id as string) series_id,
    safe_cast(erp as int64) erp
from {{ set_datalake_project("au_abs_population_staging.erp_age_sex") }} as t
