{{
    config(
        schema="world_wil_wid",
        alias="series",
        materialized="table",
        cluster_by=["country_code", "concept"],
    )
}}


select
    safe_cast(country_code as string) country_code,
    safe_cast(variable as string) variable,
    safe_cast(series_type as string) series_type,
    safe_cast(concept as string) concept,
    safe_cast(pop as string) pop,
    safe_cast(age as string) age,
    safe_cast(country_name as string) country_name,
    safe_cast(name as string) name,
    safe_cast(simple_description as string) simple_description,
    safe_cast(technical_description as string) technical_description,
    safe_cast(type_name as string) type_name,
    safe_cast(type_description as string) type_description,
    safe_cast(pop_name as string) pop_name,
    safe_cast(pop_description as string) pop_description,
    safe_cast(age_name as string) age_name,
    safe_cast(age_description as string) age_description,
    safe_cast(unit as string) unit,
    safe_cast(source as string) source,
    safe_cast(method as string) method,
    safe_cast(extrapolation as string) extrapolation,
    safe_cast(data_points as string) data_points,
    safe_cast(data_quality_score as float64) data_quality_score
from {{ set_datalake_project("world_wil_wid_staging.series") }} as t
