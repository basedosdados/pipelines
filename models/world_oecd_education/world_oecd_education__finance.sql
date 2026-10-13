{{
    config(
        alias="finance",
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
    safe_cast(financing_source as string) financing_source,
    safe_cast(expenditure_destination as string) expenditure_destination,
    safe_cast(expenditure_type as string) expenditure_type,
    safe_cast(price_base as string) price_base,
    safe_cast(unit_measure as string) unit_measure,
    safe_cast(questionnaire_sheet as string) questionnaire_sheet,
    safe_cast(obs_value as float64) obs_value,
    safe_cast(base_period as string) base_period,
    safe_cast(decimals as string) decimals,
    safe_cast(denominator_category as string) denominator_category,
    safe_cast(denominator_value as string) denominator_value,
    safe_cast(last_modified as string) last_modified,
    safe_cast(last_update as string) last_update,
    safe_cast(numerator_category as string) numerator_category,
    safe_cast(numerator_value as string) numerator_value,
    safe_cast(obs_status as string) obs_status,
    safe_cast(obs_status_2 as string) obs_status_2,
    safe_cast(obs_status_3 as string) obs_status_3,
    safe_cast(questionnaire_row_id as string) questionnaire_row_id,
    safe_cast(unit_multiplier as string) unit_multiplier,
    safe_cast(source_flow as string) source_flow,
    safe_cast(source_flow_version as string) source_flow_version
from {{ set_datalake_project("world_oecd_education_staging.finance") }} as t
