{{
    config(
        schema="world_iati_activities",
        alias="transaction_breakdown",
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
    safe_cast(transaction_type_code as string) transaction_type_code,
    safe_cast(transaction_type_name as string) transaction_type_name,
    safe_cast(transaction_date as date) transaction_date,
    safe_cast(sector_code as string) sector_code,
    safe_cast(sector_name as string) sector_name,
    safe_cast(recipient_country_code as string) recipient_country_code,
    safe_cast(recipient_country_name as string) recipient_country_name,
    safe_cast(recipient_region_code as string) recipient_region_code,
    safe_cast(recipient_region_name as string) recipient_region_name,
    safe_cast(value as float64) value,
    safe_cast(currency_code as string) currency_code,
    safe_cast(value_date as date) value_date,
    safe_cast(value_usd as float64) value_usd,
    safe_cast(percentage_used as float64) percentage_used
from
    {{ set_datalake_project("world_iati_activities_staging.transaction_breakdown") }}
    as t
