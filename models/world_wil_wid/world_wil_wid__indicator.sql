{{
    config(
        schema="world_wil_wid",
        alias="indicator",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1800, "end": 2030, "interval": 1},
        },
        cluster_by=["country_code", "concept", "percentile"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(country_code as string) country_code,
    safe_cast(variable as string) variable,
    safe_cast(series_type as string) series_type,
    safe_cast(concept as string) concept,
    safe_cast(pop as string) pop,
    safe_cast(age as string) age,
    safe_cast(percentile as string) percentile,
    safe_cast(value as float64) value,
    safe_cast(data_quality as string) data_quality
from {{ set_datalake_project("world_wil_wid_staging.indicator") }} as t
