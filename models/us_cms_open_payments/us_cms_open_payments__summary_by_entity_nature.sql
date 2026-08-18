{{
    config(
        schema="us_cms_open_payments",
        alias="summary_by_entity_nature",
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
    safe_cast(reporting_entity_id as string) reporting_entity_id,
    safe_cast(payment_nature_code as string) payment_nature_code,
    safe_cast(transaction_count as int64) transaction_count,
    safe_cast(amount_total as float64) amount_total,
    safe_cast(reporting_entity_name as string) reporting_entity_name
from {{ set_datalake_project("us_cms_open_payments_staging.summary_by_entity_nature") }} as t
