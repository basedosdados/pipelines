{{
    config(
        schema="world_iati_activities",
        alias="registry_dataset",
        materialized="table",
    )
}}


select
    safe_cast(registry_dataset_id as string) registry_dataset_id,
    safe_cast(publisher_id as string) publisher_id,
    safe_cast(licence_id as string) licence_id,
    safe_cast(source_url as string) source_url,
    safe_cast(cached_xml_url as string) cached_xml_url,
    safe_cast(is_downloaded as bool) is_downloaded,
    safe_cast(last_update_check as datetime) last_update_check
from {{ set_datalake_project("world_iati_activities_staging.registry_dataset") }} as t
