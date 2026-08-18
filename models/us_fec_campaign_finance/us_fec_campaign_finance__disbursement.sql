{{
    config(
        schema="us_fec_campaign_finance",
        alias="disbursement",
        materialized="table",
        partition_by={
            "field": "cycle",
            "data_type": "int64",
            "range": {"start": 1980, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(cycle as int64) cycle,
    safe_cast(committee_id as string) committee_id,
    safe_cast(back_reference_transaction_id as string) back_reference_transaction_id,
    safe_cast(transaction_id as string) transaction_id,
    safe_cast(sub_id as string) sub_id,
    safe_cast(file_number as string) file_number,
    safe_cast(image_number as string) image_number,
    safe_cast(amendment_indicator as string) amendment_indicator,
    safe_cast(report_year as int64) report_year,
    safe_cast(report_type as string) report_type,
    safe_cast(line_number as string) line_number,
    safe_cast(form_type as string) form_type,
    safe_cast(schedule_type as string) schedule_type,
    safe_cast(election_type_year as string) election_type_year,
    safe_cast(entity_type as string) entity_type,
    safe_cast(payee_name as string) payee_name,
    safe_cast(payee_city as string) payee_city,
    safe_cast(payee_state as string) payee_state,
    safe_cast(payee_zip_code as string) payee_zip_code,
    case
        when
            safe_cast(transaction_date as date)
            between date(1975, 1, 1) and date(safe_cast(cycle as int64) + 1, 12, 31)
        then safe_cast(transaction_date as date)
    end transaction_date,
    safe_cast(transaction_amount as float64) transaction_amount,
    safe_cast(purpose as string) purpose,
    safe_cast(category as string) category,
    safe_cast(category_description as string) category_description,
    safe_cast(memo_code as string) memo_code,
    safe_cast(memo_text as string) memo_text
from {{ set_datalake_project("us_fec_campaign_finance_staging.disbursement") }} as t
