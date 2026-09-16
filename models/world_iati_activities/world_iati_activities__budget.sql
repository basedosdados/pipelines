{{
    config(
        schema="world_iati_activities",
        alias="budget",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 0, "end": 2105, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(registry_dataset_id as string) registry_dataset_id,
    safe_cast(publisher_id as string) publisher_id,
    safe_cast(budget_id as string) budget_id,
    safe_cast(activity_id as string) activity_id,
    safe_cast(iati_identifier as string) iati_identifier,
    safe_cast(reporting_org_id as string) reporting_org_id,
    safe_cast(licence_id as string) licence_id,
    safe_cast(budget_type_code as string) budget_type_code,
    safe_cast(budget_type_name as string) budget_type_name,
    safe_cast(budget_status_code as string) budget_status_code,
    safe_cast(budget_status_name as string) budget_status_name,
    safe_cast(period_start_date as date) period_start_date,
    safe_cast(period_end_date as date) period_end_date,
    safe_cast(value as float64) value,
    safe_cast(currency_code as string) currency_code,
    safe_cast(currency_name as string) currency_name,
    safe_cast(value_date as date) value_date
from {{ set_datalake_project("world_iati_activities_staging.budget") }} as t
