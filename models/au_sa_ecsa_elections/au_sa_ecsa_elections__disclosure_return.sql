{{
    config(
        schema="au_sa_ecsa_elections",
        alias="disclosure_return",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2015, "end": 2035, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(return_id as string) return_id,
    safe_cast(portal as string) portal,
    safe_cast(return_type as string) return_type,
    safe_cast(date_lodged as date) date_lodged,
    safe_cast(submitter_name as string) submitter_name,
    safe_cast(return_for_name as string) return_for_name,
    safe_cast(recipient_name as string) recipient_name,
    safe_cast(period_start_date as date) period_start_date,
    safe_cast(period_end_date as date) period_end_date,
    safe_cast(declared_value as float64) declared_value
from {{ set_datalake_project("au_sa_ecsa_elections_staging.disclosure_return") }} as t
