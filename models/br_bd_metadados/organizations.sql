select
    safe_cast(id as string) id,
    safe_cast(name as string) name,
    safe_cast(description as string) description,
    safe_cast(display_name as string) display_name,
    safe_cast(title as string) title,
    safe_cast(package_count as int64) package_count,
    safe_cast(date_created as date) date_created,
from {{ set_datalake_project("br_bd_metadados_staging.organizations") }} as t
