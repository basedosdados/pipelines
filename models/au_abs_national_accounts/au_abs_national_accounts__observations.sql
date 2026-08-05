{{
    config(
        schema="au_abs_national_accounts",
        alias="observations",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1959, "end": 2030, "interval": 1},
        },
        cluster_by=["series_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(financial_year as string) financial_year,
    safe_cast(series_id as string) series_id,
    safe_cast(value as float64) value
from {{ set_datalake_project("au_abs_national_accounts_staging.observations") }} as t
