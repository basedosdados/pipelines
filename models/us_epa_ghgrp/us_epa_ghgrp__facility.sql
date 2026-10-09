{{
    config(
        schema="us_epa_ghgrp",
        alias="facility",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2010, "end": 2030, "interval": 1},
        },
        cluster_by=["facility_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(facility_id as string) facility_id,
    safe_cast(frs_id as string) frs_id,
    safe_cast(state_id as string) state_id,
    safe_cast(county_id as string) county_id,
    safe_cast(naics_id as string) naics_id,
    safe_cast(facility_name as string) facility_name,
    safe_cast(parent_company as string) parent_company,
    safe_cast(facility_type as string) facility_type,
    safe_cast(industry_type as string) industry_type,
    safe_cast(reporting_status as string) reporting_status,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(county_name as string) county_name,
    safe_cast(city as string) city,
    safe_cast(zip_code as string) zip_code,
    safe_cast(address as string) address,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude,
    safe_cast(cems_used as string) cems_used,
    safe_cast(co2_captured as string) co2_captured,
    safe_cast(co2_supplied as string) co2_supplied
from {{ set_datalake_project("us_epa_ghgrp_staging.facility") }} as t
