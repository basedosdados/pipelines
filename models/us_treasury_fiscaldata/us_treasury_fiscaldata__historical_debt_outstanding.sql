{{
    config(
        schema="us_treasury_fiscaldata",
        alias="historical_debt_outstanding",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1790, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(record_date as date) record_date,
    safe_cast(fiscal_year as int64) fiscal_year,
    safe_cast(debt_outstanding as float64) debt_outstanding
from
    {{
        set_datalake_project(
            "us_treasury_fiscaldata_staging.historical_debt_outstanding"
        )
    }} as t
