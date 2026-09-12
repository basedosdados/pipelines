{{
    config(
        schema="us_census_bps",
        alias="permit_county_annual",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1990, "end": 2030, "interval": 1},
        },
        cluster_by=["state_id", "structure_type"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(county_id as string) county_id,
    safe_cast(state_id as string) state_id,
    safe_cast(region_id as string) region_id,
    safe_cast(division_id as string) division_id,
    safe_cast(structure_type as string) structure_type,
    safe_cast(county_name as string) county_name,
    safe_cast(buildings as int64) buildings,
    safe_cast(units as int64) units,
    safe_cast(valuation as int64) valuation,
    safe_cast(buildings_reported as int64) buildings_reported,
    safe_cast(units_reported as int64) units_reported,
    safe_cast(valuation_reported as int64) valuation_reported
from {{ set_datalake_project("us_census_bps_staging.permit_county_annual") }} as t
