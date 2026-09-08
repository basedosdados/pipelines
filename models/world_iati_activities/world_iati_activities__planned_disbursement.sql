{{
    config(
        schema="world_iati_activities",
        alias="planned_disbursement",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 0, "end": 2069, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(registry_dataset_id as string) registry_dataset_id,
    safe_cast(publisher_id as string) publisher_id,
    safe_cast(planned_disbursement_id as string) planned_disbursement_id,
    safe_cast(activity_id as string) activity_id,
    safe_cast(iati_identifier as string) iati_identifier,
    safe_cast(reporting_org_id as string) reporting_org_id,
    safe_cast(licence_id as string) licence_id,
    safe_cast(planned_disbursement_type_code as string) planned_disbursement_type_code,
    safe_cast(planned_disbursement_type_name as string) planned_disbursement_type_name,
    safe_cast(period_start_date as date) period_start_date,
    safe_cast(period_end_date as date) period_end_date,
    safe_cast(value as float64) value,
    safe_cast(currency_code as string) currency_code,
    safe_cast(currency_name as string) currency_name,
    safe_cast(value_date as date) value_date,
    safe_cast(provider_org_id as string) provider_org_id,
    safe_cast(provider_activity_id as string) provider_activity_id,
    safe_cast(provider_org_type_code as string) provider_org_type_code,
    safe_cast(provider_org_type_name as string) provider_org_type_name,
    safe_cast(provider_org_name as string) provider_org_name,
    safe_cast(receiver_org_id as string) receiver_org_id,
    safe_cast(receiver_activity_id as string) receiver_activity_id,
    safe_cast(receiver_org_type_code as string) receiver_org_type_code,
    safe_cast(receiver_org_type_name as string) receiver_org_type_name,
    safe_cast(receiver_org_name as string) receiver_org_name
from
    {{ set_datalake_project("world_iati_activities_staging.planned_disbursement") }}
    as t
