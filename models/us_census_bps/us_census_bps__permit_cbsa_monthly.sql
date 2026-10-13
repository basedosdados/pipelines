{{
    config(
        schema="us_census_bps",
        alias="permit_cbsa_monthly",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2004, "end": 2031, "interval": 1},
        },
        cluster_by=["cbsa_id", "structure_type"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(month as int64) month,
    safe_cast(cbsa_id as string) cbsa_id,
    safe_cast(csa_id as string) csa_id,
    safe_cast(cbsa_type as string) cbsa_type,
    safe_cast(full_monthly_coverage as string) full_monthly_coverage,
    safe_cast(structure_type as string) structure_type,
    safe_cast(cbsa_name as string) cbsa_name,
    safe_cast(buildings as int64) buildings,
    safe_cast(units as int64) units,
    safe_cast(valuation as int64) valuation,
    safe_cast(buildings_reported as int64) buildings_reported,
    safe_cast(units_reported as int64) units_reported,
    safe_cast(valuation_reported as int64) valuation_reported
from {{ set_datalake_project("us_census_bps_staging.permit_cbsa_monthly") }} as t
