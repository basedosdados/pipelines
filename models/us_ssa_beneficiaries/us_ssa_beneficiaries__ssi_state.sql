{{
    config(
        alias="ssi_state",
        schema="us_ssa_beneficiaries",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1998, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(state_id as string) state_id,
    safe_cast(state_or_area as string) state_or_area,
    safe_cast(eligibility_category as string) eligibility_category,
    safe_cast(age_group as string) age_group,
    safe_cast(oasdi_concurrent as string) oasdi_concurrent,
    safe_cast(recipient_count as int64) recipient_count,
    safe_cast(recipient_count_note as string) recipient_count_note,
    safe_cast(payment_amount_month as int64) payment_amount_month,
    safe_cast(payment_amount_month_note as string) payment_amount_month_note
from {{ set_datalake_project("us_ssa_beneficiaries_staging.ssi_state") }} as t
