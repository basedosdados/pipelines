{{
    config(
        schema="au_aec_elections",
        alias="disclosure_receipt",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1998, "end": 2035, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(financial_year as string) financial_year,
    safe_cast(return_type as string) return_type,
    safe_cast(recipient_name as string) recipient_name,
    safe_cast(received_from as string) received_from,
    safe_cast(receipt_type as string) receipt_type,
    safe_cast(value as float64) value
from {{ set_datalake_project("au_aec_elections_staging.disclosure_receipt") }} as t
