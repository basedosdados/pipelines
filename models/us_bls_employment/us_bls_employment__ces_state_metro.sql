{{
    config(
        schema="us_bls_employment",
        alias="ces_state_metro",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1939, "end": 2031, "interval": 1},
        },
        cluster_by=["seasonal_adjustment", "state_id", "industry_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(month as int64) month,
    safe_cast(period_id as string) period_id,
    safe_cast(state_id as string) state_id,
    safe_cast(cbsa_id as string) cbsa_id,
    safe_cast(area_id as string) area_id,
    safe_cast(series_id as string) series_id,
    safe_cast(supersector_id as string) supersector_id,
    safe_cast(industry_id as string) industry_id,
    safe_cast(naics_id as string) naics_id,
    safe_cast(data_type_id as string) data_type_id,
    safe_cast(seasonal_adjustment as string) seasonal_adjustment,
    safe_cast(benchmark_year as int64) benchmark_year,
    safe_cast(value as float64) value,
    safe_cast(measurement_unit as string) measurement_unit,
    safe_cast(footnote_id as string) footnote_id
from {{ set_datalake_project("us_bls_employment_staging.ces_state_metro") }} as t
