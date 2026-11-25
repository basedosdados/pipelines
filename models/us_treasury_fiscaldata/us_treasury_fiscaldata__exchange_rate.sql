{{
    config(
        schema="us_treasury_fiscaldata",
        alias="exchange_rate",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2001, "end": 2031, "interval": 1},
        },
        cluster_by=["country"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(record_date as date) record_date,
    safe_cast(effective_date as date) effective_date,
    safe_cast(country as string) country,
    safe_cast(currency as string) currency,
    safe_cast(country_currency_desc as string) country_currency_desc,
    safe_cast(exchange_rate as float64) exchange_rate
from {{ set_datalake_project("us_treasury_fiscaldata_staging.exchange_rate") }} as t
