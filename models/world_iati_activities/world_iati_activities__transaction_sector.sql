{{
    config(
        schema="world_iati_activities",
        alias="transaction_sector",
        materialized="table",
    )
}}


select
    safe_cast(registry_dataset_id as string) registry_dataset_id,
    safe_cast(publisher_id as string) publisher_id,
    safe_cast(transaction_sector_id as string) transaction_sector_id,
    safe_cast(activity_id as string) activity_id,
    safe_cast(iati_identifier as string) iati_identifier,
    safe_cast(reporting_org_id as string) reporting_org_id,
    safe_cast(licence_id as string) licence_id,
    safe_cast(transaction_id as string) transaction_id,
    safe_cast(vocabulary_code as string) vocabulary_code,
    safe_cast(vocabulary_name as string) vocabulary_name,
    safe_cast(vocabulary_uri as string) vocabulary_uri,
    safe_cast(sector_code as string) sector_code,
    safe_cast(sector_name as string) sector_name,
    safe_cast(sector_narrative as string) sector_narrative
from {{ set_datalake_project("world_iati_activities_staging.transaction_sector") }} as t
