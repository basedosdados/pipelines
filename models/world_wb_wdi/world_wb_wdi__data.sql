{{
    config(
        schema="world_wb_wdi",
        alias="data",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1960, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(country_id as string) country_id,
    safe_cast(indicator_id as string) indicator_id,
    safe_cast(value as float64) value,
from {{ set_datalake_project("world_wb_wdi_staging.data") }} as t
