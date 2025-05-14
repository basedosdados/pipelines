select
    safe_cast(table_id as string) table_id,
    safe_cast(name as string) name,
    safe_cast(bigquery_type as string) bigquery_type,
    safe_cast(description as string) description,
    safe_cast(temporal_coverage as string) temporal_coverage,
    safe_cast(covered_by_dictionary as string) covered_by_dictionary,
    safe_cast(directory_column as string) directory_column,
    safe_cast(measurement_unit as string) measurement_unit,
    safe_cast(has_sensitive_data as string) has_sensitive_data,
    safe_cast(observations as string) observations,
    safe_cast(is_in_staging as string) is_in_staging,
    safe_cast(is_partition as string) is_partition
from {{ set_datalake_project("br_bd_metadados_staging.columns") }} as t
