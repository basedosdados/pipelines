{{
    config(
        schema="us_census_cog",
        alias="government_unit",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1997, "end": 2035, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(government_id as string) government_id,
    safe_cast(government_id_govs as string) government_id_govs,
    safe_cast(government_type as string) government_type,
    safe_cast(unit_category as string) unit_category,
    safe_cast(unit_name as string) unit_name,
    safe_cast(function_code as string) function_code,
    safe_cast(function_name as string) function_name,
    safe_cast(school_level_code as string) school_level_code,
    safe_cast(political_description as string) political_description,
    safe_cast(officer_title as string) officer_title,
    safe_cast(address_line_1 as string) address_line_1,
    safe_cast(address_line_2 as string) address_line_2,
    safe_cast(city as string) city,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(zip_code as string) zip_code,
    safe_cast(zip_code_extension as string) zip_code_extension,
    safe_cast(web_address as string) web_address,
    safe_cast(population as int64) population,
    safe_cast(population_year as int64) population_year,
    safe_cast(school_enrollment as int64) school_enrollment,
    safe_cast(enrollment_year as int64) enrollment_year,
    safe_cast(state_id as string) state_id,
    safe_cast(county_id as string) county_id,
    safe_cast(place_id as string) place_id,
    safe_cast(county_subdivision_id as string) county_subdivision_id,
    safe_cast(county_area_name as string) county_area_name,
    safe_cast(parent_government_id as string) parent_government_id,
    safe_cast(is_active as string) is_active
from {{ set_datalake_project("us_census_cog_staging.government_unit") }} as t
