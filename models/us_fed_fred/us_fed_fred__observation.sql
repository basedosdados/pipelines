{{
    config(
        schema="us_fed_fred",
        alias="observation",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1776, "end": 2035, "interval": 1},
        },
        cluster_by=["series_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(date as date) date,
    safe_cast(series_id as string) series_id,
    safe_cast(value as float64) value
from {{ set_datalake_project("us_fed_fred_staging.observation") }} as t
