{{
    config(
        schema="us_census_cog",
        alias="finance_unit",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1967, "end": 2035, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(government_id as string) government_id,
    safe_cast(government_id_govs as string) government_id_govs,
    safe_cast(government_type as string) government_type,
    safe_cast(unit_name as string) unit_name,
    safe_cast(state_id as string) state_id,
    safe_cast(county_id as string) county_id,
    safe_cast(county_name as string) county_name,
    safe_cast(place_id as string) place_id,
    safe_cast(census_region_code as string) census_region_code,
    safe_cast(population as int64) population,
    safe_cast(population_year as int64) population_year,
    safe_cast(school_enrollment as int64) school_enrollment,
    safe_cast(school_level_code as string) school_level_code,
    safe_cast(special_district_function_code as string) special_district_function_code,
    safe_cast(fiscal_year_end as string) fiscal_year_end,
    safe_cast(survey_weight as float64) survey_weight,
    safe_cast(data_flag as string) data_flag,
    safe_cast(is_imputed_record as string) is_imputed_record
from {{ set_datalake_project("us_census_cog_staging.finance_unit") }} as t
