{{
    config(
        schema="us_census_bps",
        alias="permit_place_monthly",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1988, "end": 2031, "interval": 1},
        },
        cluster_by=["state_id", "structure_type"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(month as int64) month,
    safe_cast(state_id as string) state_id,
    safe_cast(county_id as string) county_id,
    safe_cast(place_id as string) place_id,
    safe_cast(mcd_id as string) mcd_id,
    safe_cast(cbsa_id as string) cbsa_id,
    safe_cast(csa_id as string) csa_id,
    safe_cast(msa_cmsa_id as string) msa_cmsa_id,
    safe_cast(pmsa_id as string) pmsa_id,
    safe_cast(permit_office_id as string) permit_office_id,
    safe_cast(census_place_id as string) census_place_id,
    safe_cast(region_id as string) region_id,
    safe_cast(division_id as string) division_id,
    safe_cast(structure_type as string) structure_type,
    safe_cast(place_name as string) place_name,
    safe_cast(central_city as string) central_city,
    safe_cast(footnote_code as string) footnote_code,
    safe_cast(zip_code as string) zip_code,
    safe_cast(population as int64) population,
    safe_cast(source_code as string) source_code,
    safe_cast(buildings as int64) buildings,
    safe_cast(units as int64) units,
    safe_cast(valuation as int64) valuation
from {{ set_datalake_project("us_census_bps_staging.permit_place_monthly") }} as t
