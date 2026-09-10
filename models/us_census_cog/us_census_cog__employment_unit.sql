{{
    config(
        schema="us_census_cog",
        alias="employment_unit",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1992, "end": 2035, "interval": 1},
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
    safe_cast(census_region_code as string) census_region_code,
    safe_cast(population_enrollment_function as string) population_enrollment_function,
    safe_cast(population_enrollment_year as int64) population_enrollment_year,
    safe_cast(school_level_code as string) school_level_code,
    safe_cast(selection_probability as float64) selection_probability,
    safe_cast(worksheet_code as string) worksheet_code
from {{ set_datalake_project("us_census_cog_staging.employment_unit") }} as t
