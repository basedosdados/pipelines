{{
    config(
        schema="au_abs_productivity",
        alias="observations",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1974, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(financial_year as string) financial_year,
    safe_cast(indicator_id as string) indicator_id,
    safe_cast(value as float64) value
from {{ set_datalake_project("au_abs_productivity_staging.observations") }} as t
