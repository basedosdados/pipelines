{{
    config(
        schema="world_iati_activities",
        alias="transaction",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 0, "end": 2036, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(registry_dataset_id as string) registry_dataset_id,
    safe_cast(publisher_id as string) publisher_id,
    safe_cast(transaction_id as string) transaction_id,
    safe_cast(activity_id as string) activity_id,
    safe_cast(iati_identifier as string) iati_identifier,
    safe_cast(reporting_org_id as string) reporting_org_id,
    safe_cast(licence_id as string) licence_id,
    safe_cast(transaction_ref as string) transaction_ref,
    safe_cast(transaction_type_code as string) transaction_type_code,
    safe_cast(transaction_type_name as string) transaction_type_name,
    safe_cast(transaction_date as date) transaction_date,
    safe_cast(value as float64) value,
    safe_cast(currency_code as string) currency_code,
    safe_cast(currency_name as string) currency_name,
    safe_cast(value_date as date) value_date,
    safe_cast(value_usd as float64) value_usd,
    safe_cast(description as string) description,
    safe_cast(provider_org_id as string) provider_org_id,
    safe_cast(provider_activity_id as string) provider_activity_id,
    safe_cast(provider_org_type_code as string) provider_org_type_code,
    safe_cast(provider_org_type_name as string) provider_org_type_name,
    safe_cast(provider_org_name as string) provider_org_name,
    safe_cast(receiver_org_id as string) receiver_org_id,
    safe_cast(receiver_activity_id as string) receiver_activity_id,
    safe_cast(receiver_org_type_code as string) receiver_org_type_code,
    safe_cast(receiver_org_type_name as string) receiver_org_type_name,
    safe_cast(receiver_org_name as string) receiver_org_name,
    safe_cast(disbursement_channel_code as string) disbursement_channel_code,
    safe_cast(disbursement_channel_name as string) disbursement_channel_name,
    safe_cast(sector_code as string) sector_code,
    safe_cast(sector_name as string) sector_name,
    safe_cast(recipient_country_code as string) recipient_country_code,
    safe_cast(recipient_country_name as string) recipient_country_name,
    safe_cast(recipient_region_code as string) recipient_region_code,
    safe_cast(recipient_region_name as string) recipient_region_name,
    safe_cast(
        recipient_region_vocabulary_code as string
    ) recipient_region_vocabulary_code,
    safe_cast(
        recipient_region_vocabulary_name as string
    ) recipient_region_vocabulary_name,
    safe_cast(flow_type_code as string) flow_type_code,
    safe_cast(flow_type_name as string) flow_type_name,
    safe_cast(finance_type_code as string) finance_type_code,
    safe_cast(finance_type_name as string) finance_type_name,
    safe_cast(tied_status_code as string) tied_status_code,
    safe_cast(tied_status_name as string) tied_status_name,
    safe_cast(is_humanitarian as bool) is_humanitarian
from {{ set_datalake_project("world_iati_activities_staging.transaction") }} as t
