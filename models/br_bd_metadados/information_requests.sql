select
    safe_cast(dataset_id as string) dataset_id,
    safe_cast(id as string) id,
    safe_cast(name as string) name,
    safe_cast(date_created as date) date_created,
    safe_cast(date_last_modified as date) date_last_modified,
    safe_cast(url as string) url,
    safe_cast(origin as string) origin,
    safe_cast(number as string) number,
    safe_cast(opening_date as date) opening_date,
    safe_cast(requested_by as string) requested_by,
    safe_cast(status as string) status,
    safe_cast(data_url as string) data_url,
    safe_cast(spatial_coverage as string) spatial_coverage,
    safe_cast(temporal_coverage as string) temporal_coverage,
    safe_cast(update_frequency as string) update_frequency,
    safe_cast(observation_level as string) observation_level
from {{ set_datalake_project("br_bd_metadados_staging.information_requests") }} as t
