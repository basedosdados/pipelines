{{
    config(
        schema="us_cms_open_payments",
        alias="summary_reporting_entity",
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
    safe_cast(reporting_entity_name as string) reporting_entity_name,
    safe_cast(reporting_entity_state as string) reporting_entity_state,
    safe_cast(reporting_entity_country as string) reporting_entity_country,
    safe_cast(general_payment_amount_total as float64) general_payment_amount_total,
    safe_cast(research_payment_amount_total as float64) research_payment_amount_total,
    safe_cast(
        ownership_amount_invested_total as float64
    ) ownership_amount_invested_total,
    safe_cast(ownership_interest_value_total as float64) ownership_interest_value_total,
    safe_cast(general_transaction_count as int64) general_transaction_count,
    safe_cast(
        general_transaction_count_physician as int64
    ) general_transaction_count_physician,
    safe_cast(
        general_transaction_count_non_physician_practitioner as int64
    ) general_transaction_count_non_physician_practitioner,
    safe_cast(
        general_transaction_count_teaching_hospital as int64
    ) general_transaction_count_teaching_hospital,
    safe_cast(research_transaction_count as int64) research_transaction_count,
    safe_cast(
        research_transaction_count_physician as int64
    ) research_transaction_count_physician,
    safe_cast(
        research_transaction_count_non_physician_practitioner as int64
    ) research_transaction_count_non_physician_practitioner,
    safe_cast(
        research_transaction_count_teaching_hospital as int64
    ) research_transaction_count_teaching_hospital,
    safe_cast(
        ownership_invested_transaction_count as int64
    ) ownership_invested_transaction_count,
    safe_cast(disputed_transaction_count as int64) disputed_transaction_count,
    safe_cast(undisputed_transaction_count as int64) undisputed_transaction_count
from
    {{ set_datalake_project("us_cms_open_payments_staging.summary_reporting_entity") }}
    as t
