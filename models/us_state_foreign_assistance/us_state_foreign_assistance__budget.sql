{{
    config(
        schema="us_state_foreign_assistance",
        alias="budget",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2004, "end": 2029, "interval": 1},
        },
    )
}}

select
    safe_cast(year as int64) year,
    safe_cast(transaction_type_id as string) transaction_type_id,
    safe_cast(country_iso3_code as string) country_iso3_code,
    safe_cast(country_id as string) country_id,
    safe_cast(country_code as string) country_code,
    safe_cast(region_id as string) region_id,
    safe_cast(income_group_id as string) income_group_id,
    safe_cast(managing_subagency_id as string) managing_subagency_id,
    safe_cast(operating_unit as string) operating_unit,
    safe_cast(funding_agency_id as string) funding_agency_id,
    safe_cast(funding_account_id as string) funding_account_id,
    safe_cast(international_category_id as string) international_category_id,
    safe_cast(international_sector_code as string) international_sector_code,
    safe_cast(international_purpose_code as string) international_purpose_code,
    safe_cast(us_category_id as string) us_category_id,
    safe_cast(us_sector_id as string) us_sector_id,
    safe_cast(oco_flag as string) oco_flag,
    safe_cast(activity_id as string) activity_id,
    safe_cast(activity_name as string) activity_name,
    safe_cast(activity_description as string) activity_description,
    safe_cast(current_amount as float64) current_amount,
    safe_cast(constant_amount as float64) constant_amount
from {{ set_datalake_project("us_state_foreign_assistance_staging.budget") }} as t
