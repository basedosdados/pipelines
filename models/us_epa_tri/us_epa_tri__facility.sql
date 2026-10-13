{{
    config(
        schema="us_epa_tri",
        alias="facility",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1987, "end": 2035, "interval": 1},
        },
        cluster_by=["tri_facility_id"],
    )
}}

-- Atualizado em 2026-09-03
select
    safe_cast(year as int64) year,
    safe_cast(tri_facility_id as string) tri_facility_id,
    safe_cast(frs_id as string) frs_id,
    safe_cast(facility_name as string) facility_name,
    safe_cast(street_address as string) street_address,
    safe_cast(city as string) city,
    safe_cast(county_id as string) county_id,
    safe_cast(county_name as string) county_name,
    safe_cast(state as string) state,
    safe_cast(zip_code as string) zip_code,
    safe_cast(bia_code as string) bia_code,
    safe_cast(tribe_name as string) tribe_name,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude,
    safe_cast(parent_company_name as string) parent_company_name,
    safe_cast(parent_company_duns as string) parent_company_duns,
    safe_cast(
        standardized_parent_company_name as string
    ) standardized_parent_company_name,
    safe_cast(foreign_parent_company_name as string) foreign_parent_company_name,
    safe_cast(foreign_parent_company_duns as string) foreign_parent_company_duns,
    safe_cast(
        standardized_foreign_parent_company_name as string
    ) standardized_foreign_parent_company_name,
    safe_cast(federal_facility as string) federal_facility
from {{ set_datalake_project("us_epa_tri_staging.facility") }} as t
