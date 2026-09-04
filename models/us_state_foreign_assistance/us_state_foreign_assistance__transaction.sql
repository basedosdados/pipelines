{{
    config(
        schema="us_state_foreign_assistance",
        alias="transaction",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1946, "end": 2031, "interval": 1},
        },
    )
}}

select
    safe_cast(year as int64) year,
    safe_cast(fiscal_period as string) fiscal_period,
    safe_cast(transaction_date as date) transaction_date,
    safe_cast(transaction_type_id as string) transaction_type_id,
    safe_cast(country_iso3_code as string) country_iso3_code,
    safe_cast(country_id as string) country_id,
    safe_cast(country_code as string) country_code,
    safe_cast(region_id as string) region_id,
    safe_cast(income_group_id as string) income_group_id,
    safe_cast(managing_agency_id as string) managing_agency_id,
    safe_cast(managing_subagency_id as string) managing_subagency_id,
    safe_cast(funding_agency_id as string) funding_agency_id,
    safe_cast(funding_account_id as string) funding_account_id,
    safe_cast(implementing_partner_id as string) implementing_partner_id,
    safe_cast(implementing_partner_name as string) implementing_partner_name,
    safe_cast(
        implementing_partner_category_id as string
    ) implementing_partner_category_id,
    safe_cast(
        implementing_partner_subcategory_id as string
    ) implementing_partner_subcategory_id,
    safe_cast(international_category_id as string) international_category_id,
    safe_cast(international_sector_code as string) international_sector_code,
    safe_cast(international_purpose_code as string) international_purpose_code,
    safe_cast(us_category_id as string) us_category_id,
    safe_cast(us_sector_id as string) us_sector_id,
    safe_cast(objective_id as string) objective_id,
    safe_cast(aid_type_group_id as string) aid_type_group_id,
    safe_cast(aid_type_id as string) aid_type_id,
    safe_cast(activity_id as string) activity_id,
    safe_cast(submission_id as string) submission_id,
    safe_cast(activity_name as string) activity_name,
    safe_cast(activity_description as string) activity_description,
    safe_cast(activity_project_number as string) activity_project_number,
    safe_cast(activity_start_date as date) activity_start_date,
    safe_cast(activity_end_date as date) activity_end_date,
    safe_cast(activity_budget_amount as float64) activity_budget_amount,
    safe_cast(current_amount as float64) current_amount,
    safe_cast(constant_amount as float64) constant_amount
from {{ set_datalake_project("us_state_foreign_assistance_staging.transaction") }} as t
