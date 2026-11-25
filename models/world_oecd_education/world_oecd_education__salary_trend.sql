{{
    config(
        alias="salary_trend",
        schema="world_oecd_education",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2000, "end": 2030, "interval": 1},
        },
        cluster_by=["country_iso3_code"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(country_iso3_code as string) country_iso3_code,
    safe_cast(reference_area as string) reference_area,
    safe_cast(measure as string) measure,
    safe_cast(unit_measure as string) unit_measure,
    safe_cast(education_institution_type as string) education_institution_type,
    safe_cast(education_level as string) education_level,
    safe_cast(age as string) age,
    safe_cast(sex as string) sex,
    safe_cast(personnel_type as string) personnel_type,
    safe_cast(personnel_qualification_level as string) personnel_qualification_level,
    safe_cast(personnel_experience_level as string) personnel_experience_level,
    safe_cast(price_base as string) price_base,
    safe_cast(obs_value as float64) obs_value,
    safe_cast(base_period as string) base_period,
    safe_cast(currency as string) currency,
    safe_cast(decimals as string) decimals,
    safe_cast(obs_status as string) obs_status,
    safe_cast(statistical_operation as string) statistical_operation,
    safe_cast(transformation as string) transformation,
    safe_cast(unit_multiplier as string) unit_multiplier,
    safe_cast(source_flow as string) source_flow,
    safe_cast(source_flow_version as string) source_flow_version
from {{ set_datalake_project("world_oecd_education_staging.salary_trend") }} as t
