{{
    config(
        schema="world_iati_activities",
        alias="document_link",
        materialized="table",
    )
}}


select
    safe_cast(registry_dataset_id as string) registry_dataset_id,
    safe_cast(publisher_id as string) publisher_id,
    safe_cast(document_link_id as string) document_link_id,
    safe_cast(activity_id as string) activity_id,
    safe_cast(iati_identifier as string) iati_identifier,
    safe_cast(reporting_org_id as string) reporting_org_id,
    safe_cast(licence_id as string) licence_id,
    safe_cast(url as string) url,
    safe_cast(format_code as string) format_code,
    safe_cast(format_name as string) format_name,
    safe_cast(title as string) title,
    safe_cast(description as string) description,
    safe_cast(document_date as date) document_date
from {{ set_datalake_project("world_iati_activities_staging.document_link") }} as t
