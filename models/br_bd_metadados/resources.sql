select
    safe_cast(dataset_id as string) dataset_id,
    safe_cast(id as string) id,
    safe_cast(name as string) name,
    safe_cast(date_created as date) date_created,
    safe_cast(date_last_modified as date) date_last_modified,
    safe_cast(type as string) type
from {{ project_path("br_bd_metadados_staging.resources") }} as t
