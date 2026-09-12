{{
    config(
        schema="world_iati_activities",
        alias="participating_org",
        materialized="table",
    )
}}


select
    safe_cast(registry_dataset_id as string) registry_dataset_id,
    safe_cast(publisher_id as string) publisher_id,
    safe_cast(participating_org_id as string) participating_org_id,
    safe_cast(activity_id as string) activity_id,
    safe_cast(iati_identifier as string) iati_identifier,
    safe_cast(reporting_org_id as string) reporting_org_id,
    safe_cast(licence_id as string) licence_id,
    safe_cast(org_id as string) org_id,
    safe_cast(org_activity_id as string) org_activity_id,
    safe_cast(role_code as string) role_code,
    safe_cast(role_name as string) role_name,
    safe_cast(org_type_code as string) org_type_code,
    safe_cast(org_type_name as string) org_type_name,
    safe_cast(crs_channel_code as string) crs_channel_code,
    safe_cast(crs_channel_name as string) crs_channel_name,
    safe_cast(org_name as string) org_name
from {{ set_datalake_project("world_iati_activities_staging.participating_org") }} as t
