{{
    config(
        schema="us_fec_campaign_finance",
        alias="committee_transaction",
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
    safe_cast(other_id as string) other_id,
    safe_cast(transaction_id as string) transaction_id,
    safe_cast(sub_id as string) sub_id,
    safe_cast(file_number as string) file_number,
    safe_cast(image_number as string) image_number,
    safe_cast(amendment_indicator as string) amendment_indicator,
    safe_cast(report_type as string) report_type,
    safe_cast(election_type_year as string) election_type_year,
    safe_cast(transaction_type as string) transaction_type,
    safe_cast(entity_type as string) entity_type,
    safe_cast(counterparty_name as string) counterparty_name,
    safe_cast(counterparty_city as string) counterparty_city,
    safe_cast(counterparty_state as string) counterparty_state,
    safe_cast(counterparty_zip_code as string) counterparty_zip_code,
    safe_cast(counterparty_employer as string) counterparty_employer,
    safe_cast(counterparty_occupation as string) counterparty_occupation,
    safe_cast(transaction_date as date) transaction_date,
    safe_cast(transaction_amount as float64) transaction_amount,
    safe_cast(memo_code as string) memo_code,
    safe_cast(memo_text as string) memo_text
from
    {{ set_datalake_project("us_fec_campaign_finance_staging.committee_transaction") }}
    as t
