{{
    config(
        schema="au_abs_prices_inflation",
        alias="wage_price_index",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1997, "end": 2031, "interval": 1},
        },
        cluster_by=["series_type", "region", "industry"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(quarter as int64) quarter,
    safe_cast(financial_year as string) financial_year,
    safe_cast(series_id as string) series_id,
    safe_cast(statistic as string) statistic,
    safe_cast(series_type as string) series_type,
    safe_cast(pay_measure as string) pay_measure,
    safe_cast(region as string) region,
    safe_cast(sector as string) sector,
    safe_cast(industry as string) industry,
    safe_cast(frequency as string) frequency,
    safe_cast(unit as string) unit,
    safe_cast(value as float64) value
from {{ set_datalake_project("au_abs_prices_inflation_staging.wage_price_index") }} as t
