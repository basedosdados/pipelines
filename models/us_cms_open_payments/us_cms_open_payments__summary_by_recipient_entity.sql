{{
    config(
        schema="us_cms_open_payments",
        alias="summary_by_recipient_entity",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2019, "end": 2030, "interval": 1},
        },
        cluster_by=["recipient_id", "reporting_entity_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(covered_recipient_npi as string) covered_recipient_npi,
    safe_cast(reporting_entity_id as string) reporting_entity_id,
    safe_cast(recipient_id as string) recipient_id,
    safe_cast(recipient_type as string) recipient_type,
    safe_cast(first_name as string) first_name,
    safe_cast(middle_name as string) middle_name,
    safe_cast(last_name as string) last_name,
    safe_cast(teaching_hospital_name as string) teaching_hospital_name,
    safe_cast(payment_type as string) payment_type,
    safe_cast(reporting_entity_name as string) reporting_entity_name,
    safe_cast(transaction_count as int64) transaction_count,
    safe_cast(amount_total as float64) amount_total
from {{ set_datalake_project("us_cms_open_payments_staging.summary_by_recipient_entity") }} as t
