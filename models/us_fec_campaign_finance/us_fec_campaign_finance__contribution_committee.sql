{{
    config(
        schema="us_fec_campaign_finance",
        alias="contribution_committee",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1980, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(committee_id as string) committee_id,
    safe_cast(other_id as string) other_id,
    safe_cast(candidate_id as string) candidate_id,
    safe_cast(transaction_id as string) transaction_id,
    safe_cast(sub_id as string) sub_id,
    safe_cast(file_number as string) file_number,
    safe_cast(image_number as string) image_number,
    safe_cast(amendment_indicator as string) amendment_indicator,
    safe_cast(report_type as string) report_type,
    safe_cast(election_type_year as string) election_type_year,
    safe_cast(transaction_type as string) transaction_type,
    safe_cast(entity_type as string) entity_type,
    safe_cast(contributor_name as string) contributor_name,
    safe_cast(contributor_city as string) contributor_city,
    safe_cast(contributor_state as string) contributor_state,
    safe_cast(contributor_zip_code as string) contributor_zip_code,
    safe_cast(contributor_employer as string) contributor_employer,
    safe_cast(contributor_occupation as string) contributor_occupation,
    case
        when
            safe_cast(transaction_date as date)
            between date(1975, 1, 1) and date(safe_cast(year as int64), 12, 31)
        then safe_cast(transaction_date as date)
    end transaction_date,
    safe_cast(transaction_amount as float64) transaction_amount,
    safe_cast(memo_code as string) memo_code,
    safe_cast(memo_text as string) memo_text
from
    {{ set_datalake_project("us_fec_campaign_finance_staging.contribution_committee") }}
    as t
