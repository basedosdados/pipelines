{{
    config(
        schema="us_census_bps",
        alias="permit_msa_annual",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1980, "end": 2007, "interval": 1},
        },
        cluster_by=["msa_cmsa_id", "structure_type"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(msa_cmsa_id as string) msa_cmsa_id,
    safe_cast(pmsa_id as string) pmsa_id,
    safe_cast(structure_type as string) structure_type,
    safe_cast(msa_name as string) msa_name,
    safe_cast(buildings as int64) buildings,
    safe_cast(units as int64) units,
    safe_cast(valuation as int64) valuation,
    safe_cast(buildings_reported as int64) buildings_reported,
    safe_cast(units_reported as int64) units_reported,
    safe_cast(valuation_reported as int64) valuation_reported
from {{ set_datalake_project("us_census_bps_staging.permit_msa_annual") }} as t
