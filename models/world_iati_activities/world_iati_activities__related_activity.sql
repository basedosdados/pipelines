{{
    config(
        schema="world_iati_activities",
        alias="related_activity",
        materialized="table",
    )
}}


select
    safe_cast(registry_dataset_id as string) registry_dataset_id,
    safe_cast(publisher_id as string) publisher_id,
    safe_cast(related_activity_id as string) related_activity_id,
    safe_cast(activity_id as string) activity_id,
    safe_cast(iati_identifier as string) iati_identifier,
    safe_cast(reporting_org_id as string) reporting_org_id,
    safe_cast(licence_id as string) licence_id,
    safe_cast(related_iati_identifier as string) related_iati_identifier,
    safe_cast(relation_type_code as string) relation_type_code,
    safe_cast(relation_type_name as string) relation_type_name
from {{ set_datalake_project("world_iati_activities_staging.related_activity") }} as t
