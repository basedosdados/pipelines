{{
    config(
        alias="student",
        schema="world_oecd_education",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2005, "end": 2029, "interval": 1},
        },
        cluster_by=["country_iso3_code"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(country_iso3_code as string) country_iso3_code,
    safe_cast(reference_area as string) reference_area,
    safe_cast(education_level as string) education_level,
    safe_cast(measure as string) measure,
    safe_cast(education_type as string) education_type,
    safe_cast(intensity as string) intensity,
    safe_cast(education_field as string) education_field,
    safe_cast(grade as string) grade,
    safe_cast(frequency as string) frequency,
    safe_cast(origin_area as string) origin_area,
    safe_cast(destination_area as string) destination_area,
    safe_cast(education_institution_type as string) education_institution_type,
    safe_cast(mobility as string) mobility,
    safe_cast(unit_measure as string) unit_measure,
    safe_cast(sex as string) sex,
    safe_cast(age as string) age,
    safe_cast(obs_value as float64) obs_value,
    safe_cast(obs_comment as string) obs_comment,
    safe_cast(confidentiality_status as string) confidentiality_status,
    safe_cast(decimals as string) decimals,
    safe_cast(obs_status as string) obs_status,
    safe_cast(origin_criterion as string) origin_criterion,
    safe_cast(reference_date_ages as string) reference_date_ages,
    safe_cast(reporting_year_end as string) reporting_year_end,
    safe_cast(reporting_year_start as string) reporting_year_start,
    safe_cast(time_period_collection as string) time_period_collection,
    safe_cast(unit_multiplier as string) unit_multiplier,
    safe_cast(source_flow as string) source_flow,
    safe_cast(source_flow_version as string) source_flow_version
from {{ set_datalake_project("world_oecd_education_staging.student") }} as t
