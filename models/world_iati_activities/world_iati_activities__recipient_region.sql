{{
    config(
        schema="world_iati_activities",
        alias="recipient_region",
        materialized="table",
    )
}}


select
    safe_cast(registry_dataset_id as string) registry_dataset_id,
    safe_cast(publisher_id as string) publisher_id,
    safe_cast(activity_recipient_region_id as string) activity_recipient_region_id,
    safe_cast(activity_id as string) activity_id,
    safe_cast(iati_identifier as string) iati_identifier,
    safe_cast(reporting_org_id as string) reporting_org_id,
    safe_cast(licence_id as string) licence_id,
    safe_cast(recipient_region_code as string) recipient_region_code,
    safe_cast(recipient_region_name as string) recipient_region_name,
    safe_cast(vocabulary_code as string) vocabulary_code,
    safe_cast(vocabulary_name as string) vocabulary_name,
    safe_cast(vocabulary_uri as string) vocabulary_uri,
    safe_cast(percentage as float64) percentage,
    safe_cast(recipient_region_narrative as string) recipient_region_narrative
from {{ set_datalake_project("world_iati_activities_staging.recipient_region") }} as t
