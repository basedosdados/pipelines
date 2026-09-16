{{
    config(
        alias="working_time_trend",
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
    safe_cast(personnel_type as string) personnel_type,
    safe_cast(obs_value as float64) obs_value,
    safe_cast(decimals as string) decimals,
    safe_cast(obs_status as string) obs_status,
    safe_cast(statistical_operation as string) statistical_operation,
    safe_cast(unit_multiplier as string) unit_multiplier,
    safe_cast(source_flow as string) source_flow,
    safe_cast(source_flow_version as string) source_flow_version
from {{ set_datalake_project("world_oecd_education_staging.working_time_trend") }} as t
