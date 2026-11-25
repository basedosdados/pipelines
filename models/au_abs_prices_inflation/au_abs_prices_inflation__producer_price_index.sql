{{
    config(
        schema="au_abs_prices_inflation",
        alias="producer_price_index",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1966, "end": 2031, "interval": 1},
        },
        cluster_by=["index_type", "item_name"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(quarter as int64) quarter,
    safe_cast(series_id as string) series_id,
    safe_cast(statistic as string) statistic,
    safe_cast(index_type as string) index_type,
    safe_cast(item_code as string) item_code,
    safe_cast(item_name as string) item_name,
    safe_cast(region as string) region,
    safe_cast(source_table as string) source_table,
    safe_cast(unit as string) unit,
    safe_cast(value as float64) value
from
    {{ set_datalake_project("au_abs_prices_inflation_staging.producer_price_index") }}
    as t
