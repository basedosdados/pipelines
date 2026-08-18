{{
    config(
        schema="us_cms_open_payments",
        alias="summary_teaching_hospital",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2019, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(teaching_hospital_ccn as string) teaching_hospital_ccn,
    safe_cast(teaching_hospital_name as string) teaching_hospital_name,
    safe_cast(address_line_1 as string) address_line_1,
    safe_cast(address_line_2 as string) address_line_2,
    safe_cast(city as string) city,
    safe_cast(state as string) state,
    safe_cast(zip_code as string) zip_code,
    safe_cast(general_payment_amount_total as float64) general_payment_amount_total,
    safe_cast(research_payment_amount_total as float64) research_payment_amount_total,
    safe_cast(general_transaction_count as int64) general_transaction_count,
    safe_cast(research_transaction_count as int64) research_transaction_count,
    safe_cast(disputed_transaction_count as int64) disputed_transaction_count,
    safe_cast(undisputed_transaction_count as int64) undisputed_transaction_count
from {{ set_datalake_project("us_cms_open_payments_staging.summary_teaching_hospital") }} as t
