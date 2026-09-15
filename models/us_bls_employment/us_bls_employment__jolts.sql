{{
    config(
        schema="us_bls_employment",
        alias="jolts",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2000, "end": 2031, "interval": 1},
        },
        cluster_by=["seasonal_adjustment", "industry_id", "dataelement_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(month as int64) month,
    safe_cast(period_id as string) period_id,
    safe_cast(state_id as string) state_id,
    safe_cast(region_id as string) region_id,
    safe_cast(series_id as string) series_id,
    safe_cast(industry_id as string) industry_id,
    safe_cast(sizeclass_id as string) sizeclass_id,
    safe_cast(dataelement_id as string) dataelement_id,
    safe_cast(ratelevel_id as string) ratelevel_id,
    safe_cast(seasonal_adjustment as string) seasonal_adjustment,
    safe_cast(value as float64) value,
    safe_cast(measurement_unit as string) measurement_unit,
    safe_cast(footnote_id as string) footnote_id
from {{ set_datalake_project("us_bls_employment_staging.jolts") }} as t
