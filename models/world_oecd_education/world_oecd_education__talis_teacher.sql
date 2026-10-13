{{
    config(
        alias="talis_teacher",
        schema="world_oecd_education",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2024, "end": 2029, "interval": 1},
        },
        cluster_by=["country_iso3_code"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(country_iso3_code as string) country_iso3_code,
    safe_cast(reference_area as string) reference_area,
    safe_cast(frequency as string) frequency,
    safe_cast(measure as string) measure,
    safe_cast(unit_measure as string) unit_measure,
    safe_cast(statistical_operation as string) statistical_operation,
    safe_cast(education_level as string) education_level,
    safe_cast(age as string) age,
    safe_cast(sex as string) sex,
    safe_cast(urbanisation_degree as string) urbanisation_degree,
    safe_cast(institution_type as string) institution_type,
    safe_cast(students_characteristics as string) students_characteristics,
    safe_cast(teacher_profile as string) teacher_profile,
    safe_cast(obs_value as float64) obs_value,
    safe_cast(talis_question as string) talis_question,
    safe_cast(decimals as string) decimals,
    safe_cast(obs_status as string) obs_status,
    safe_cast(unit_multiplier as string) unit_multiplier,
    safe_cast(source_flow as string) source_flow,
    safe_cast(source_flow_version as string) source_flow_version
from {{ set_datalake_project("world_oecd_education_staging.talis_teacher") }} as t
