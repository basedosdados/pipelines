{{
    config(
        schema="au_qld_ecq_elections",
        alias="disclosure_return",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2035, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(date_created as date) date_created,
    safe_cast(submitter_name as string) submitter_name,
    safe_cast(return_for_name as string) return_for_name,
    safe_cast(period_start_date as date) period_start_date,
    safe_cast(period_end_date as date) period_end_date,
    safe_cast(period_label as string) period_label,
    safe_cast(amount_received as float64) amount_received,
    safe_cast(amount_paid as float64) amount_paid
from {{ set_datalake_project("au_qld_ecq_elections_staging.disclosure_return") }} as t
