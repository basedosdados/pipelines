{{
    config(
        schema="us_census_bps",
        alias="permit_state_monthly",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1988, "end": 2031, "interval": 1},
        },
        cluster_by=["geography_level", "structure_type"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(month as int64) month,
    safe_cast(geography_level as string) geography_level,
    safe_cast(geography_id as string) geography_id,
    safe_cast(state_id as string) state_id,
    safe_cast(region_id as string) region_id,
    safe_cast(division_id as string) division_id,
    safe_cast(structure_type as string) structure_type,
    safe_cast(geography_name as string) geography_name,
    safe_cast(buildings as int64) buildings,
    safe_cast(units as int64) units,
    safe_cast(valuation as int64) valuation,
    safe_cast(buildings_reported as int64) buildings_reported,
    safe_cast(units_reported as int64) units_reported,
    safe_cast(valuation_reported as int64) valuation_reported
from {{ set_datalake_project("us_census_bps_staging.permit_state_monthly") }} as t
