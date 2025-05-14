select
    safe_cast(dataset_id as string) dataset_id,
    safe_cast(id as string) id,
    safe_cast(name as string) name,
    safe_cast(date_created as date) date_created,
    safe_cast(date_last_modified as date) date_last_modified,
    safe_cast(url as string) url,
    safe_cast(language as string) language,
    safe_cast(has_structured_data as string) has_structured_data,
    safe_cast(has_api as string) has_api,
    safe_cast(is_free as string) is_free,
    safe_cast(requires_registration as string) requires_registration,
    safe_cast(availability as string) availability,
    safe_cast(spatial_coverage as string) spatial_coverage,
    safe_cast(temporal_coverage as string) temporal_coverage,
    safe_cast(update_frequency as string) update_frequency,
    safe_cast(observation_level as string) observation_level
from {{ set_datalake_project("br_bd_metadados_staging.external_links") }} as t
