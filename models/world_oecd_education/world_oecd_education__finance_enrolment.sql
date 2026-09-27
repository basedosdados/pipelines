{{
    config(
        alias="finance_enrolment",
        schema="world_oecd_education",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1995, "end": 2029, "interval": 1},
        },
        cluster_by=["country_iso3_code"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(country_iso3_code as string) country_iso3_code,
    safe_cast(reference_area as string) reference_area,
    safe_cast(measure as string) measure,
    safe_cast(education_level as string) education_level,
    safe_cast(intensity as string) intensity,
    safe_cast(education_institution_type as string) education_institution_type,
    safe_cast(unit_measure as string) unit_measure,
    safe_cast(questionnaire_sheet as string) questionnaire_sheet,
    safe_cast(obs_value as float64) obs_value,
    safe_cast(decimals as string) decimals,
    safe_cast(last_modified as string) last_modified,
    safe_cast(last_update as string) last_update,
    safe_cast(obs_status as string) obs_status,
    safe_cast(obs_status_2 as string) obs_status_2,
    safe_cast(obs_status_3 as string) obs_status_3,
    safe_cast(questionnaire_row_id as string) questionnaire_row_id,
    safe_cast(unit_multiplier as string) unit_multiplier,
    safe_cast(source_flow as string) source_flow,
    safe_cast(source_flow_version as string) source_flow_version
from {{ set_datalake_project("world_oecd_education_staging.finance_enrolment") }} as t
