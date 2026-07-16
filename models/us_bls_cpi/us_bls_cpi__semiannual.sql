{{
    config(
        schema="us_bls_cpi",
        alias="semiannual",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1913, "end": 2031, "interval": 1},
        },
        cluster_by=["survey", "area_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(half as int64) half,
    safe_cast(survey as string) survey,
    safe_cast(seasonal_adjustment as string) seasonal_adjustment,
    safe_cast(area_id as string) area_id,
    safe_cast(item_id as string) item_id,
    safe_cast(base_period as string) base_period,
    safe_cast(index_value as float64) index_value,
    safe_cast(semiannual_change as float64) semiannual_change
from {{ set_datalake_project("us_bls_cpi_staging.semiannual") }} as t
