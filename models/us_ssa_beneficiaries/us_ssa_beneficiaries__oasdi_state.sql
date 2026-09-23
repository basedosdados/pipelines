{{
    config(
        alias="oasdi_state",
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
    safe_cast(benefit_type as string) benefit_type,
    safe_cast(age_group as string) age_group,
    safe_cast(sex as string) sex,
    safe_cast(beneficiary_count as int64) beneficiary_count,
    safe_cast(beneficiary_count_note as string) beneficiary_count_note,
    safe_cast(benefit_amount_month as int64) benefit_amount_month,
    safe_cast(benefit_amount_month_note as string) benefit_amount_month_note
from
    {{ set_datalake_project("us_ssa_beneficiaries_staging.oasdi_state") }}
    as t
