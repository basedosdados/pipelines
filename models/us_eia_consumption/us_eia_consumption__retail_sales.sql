{{
    config(
        schema="us_eia_consumption",
        alias="retail_sales",
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
    safe_cast(utility_id as string) utility_id,
    safe_cast(utility_name as string) utility_name,
    safe_cast(state_id as string) state_id,
    safe_cast(ownership_type as string) ownership_type,
    safe_cast(ba_code as string) ba_code,
    safe_cast(part as string) part,
    safe_cast(service_type as string) service_type,
    safe_cast(data_type as string) data_type,
    safe_cast(customer_sector as string) customer_sector,
    safe_cast(sales_mwh as float64) sales_mwh,
    safe_cast(revenue_usd as float64) revenue_usd,
    safe_cast(customer_count as int64) customer_count,
    safe_cast(average_price_cents_kwh as float64) average_price_cents_kwh
from {{ set_datalake_project("us_eia_consumption_staging.retail_sales") }} as t
