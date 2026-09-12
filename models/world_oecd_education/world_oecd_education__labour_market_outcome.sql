{{
    config(
        alias="labour_market_outcome",
        schema="world_oecd_education",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1981, "end": 2030, "interval": 1},
        },
        cluster_by=["country_iso3_code"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(country_iso3_code as string) country_iso3_code,
    safe_cast(reference_area as string) reference_area,
    safe_cast(sex as string) sex,
    safe_cast(age as string) age,
    safe_cast(attainment_level as string) attainment_level,
    safe_cast(education_field as string) education_field,
    safe_cast(measure as string) measure,
    safe_cast(income as string) income,
    safe_cast(birth_place as string) birth_place,
    safe_cast(migration_age as string) migration_age,
    safe_cast(education_status as string) education_status,
    safe_cast(labour_force_status as string) labour_force_status,
    safe_cast(unemployment_duration as string) unemployment_duration,
    safe_cast(unit_measure as string) unit_measure,
    safe_cast(statistical_operation as string) statistical_operation,
    safe_cast(work_time_arrangement as string) work_time_arrangement,
    safe_cast(questionnaire as string) questionnaire,
    safe_cast(frequency as string) frequency,
    safe_cast(obs_value as float64) obs_value,
    safe_cast(confidentiality_status as string) confidentiality_status,
    safe_cast(decimals as string) decimals,
    safe_cast(obs_status as string) obs_status,
    safe_cast(unit_multiplier as string) unit_multiplier,
    safe_cast(source_flow as string) source_flow,
    safe_cast(source_flow_version as string) source_flow_version
from
    {{ set_datalake_project("world_oecd_education_staging.labour_market_outcome") }}
    as t
