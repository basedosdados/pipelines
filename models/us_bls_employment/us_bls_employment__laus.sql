{{
    config(
        schema="us_bls_employment",
        alias="laus",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1976, "end": 2031, "interval": 1},
        },
        cluster_by=["seasonal_adjustment", "state_id", "measure_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(month as int64) month,
    safe_cast(period_id as string) period_id,
    safe_cast(state_id as string) state_id,
    safe_cast(county_id as string) county_id,
    safe_cast(cbsa_id as string) cbsa_id,
    safe_cast(area_type_id as string) area_type_id,
    safe_cast(area_id as string) area_id,
    safe_cast(series_id as string) series_id,
    safe_cast(measure_id as string) measure_id,
    safe_cast(seasonal_adjustment as string) seasonal_adjustment,
    safe_cast(value as float64) value,
    safe_cast(measurement_unit as string) measurement_unit,
    safe_cast(footnote_id as string) footnote_id
from {{ set_datalake_project("us_bls_employment_staging.laus") }} as t
