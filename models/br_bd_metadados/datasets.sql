select
    safe_cast(organization_id as string) organization_id,
    safe_cast(id as string) id,
    safe_cast(name as string) name,
    safe_cast(title as string) title,
    safe_cast(date_created as date) date_created,
    safe_cast(date_last_modified as date) date_last_modified,
    safe_cast(themes as string) themes,
    safe_cast(tags as string) tags
from {{ set_datalake_project("br_bd_metadados_staging.datasets") }}
