{{
    config(
        schema="us_eia_consumption",
        alias="eia861m",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1990, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(month as int64) month,
    safe_cast(state_id as string) state_id,
    safe_cast(state_code as string) state_code,
    safe_cast(customer_sector as string) customer_sector,
    safe_cast(data_status as string) data_status,
    safe_cast(sales_mwh as float64) sales_mwh,
    safe_cast(revenue_usd as float64) revenue_usd,
    safe_cast(customer_count as int64) customer_count,
    safe_cast(average_price_cents_kwh as float64) average_price_cents_kwh
from {{ set_datalake_project("us_eia_consumption_staging.eia861m") }} as t
