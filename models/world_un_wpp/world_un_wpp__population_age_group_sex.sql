{{
    config(
        schema="world_un_wpp",
        alias="population_age_group_sex",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1950, "end": 2106, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(country_iso3_code as string) country_iso3_code,
    safe_cast(age_group as string) age_group,
    safe_cast(population_male as float64) population_male,
    safe_cast(population_female as float64) population_female,
    safe_cast(population_total as float64) population_total
from {{ set_datalake_project("world_un_wpp_staging.population_age_group_sex") }} as t
