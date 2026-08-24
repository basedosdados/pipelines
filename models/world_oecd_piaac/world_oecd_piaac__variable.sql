{{
    config(
        alias="variable",
        schema="world_oecd_piaac",
        materialized="table",
    )
}}


select
    nullif(trim(safe_cast(cycle as string)), '') cycle,
    nullif(trim(safe_cast(variable_name as string)), '') variable_name,
    nullif(trim(safe_cast(table_id as string)), '') table_id,
    nullif(trim(safe_cast(column_name as string)), '') column_name,
    nullif(trim(safe_cast(label as string)), '') label,
    nullif(trim(safe_cast(domain as string)), '') domain,
    nullif(trim(safe_cast(level as string)), '') level,
    nullif(trim(safe_cast(bigquery_type as string)), '') bigquery_type,
    nullif(trim(safe_cast(measurement_unit as string)), '') measurement_unit,
    nullif(trim(safe_cast(item_code as string)), '') item_code,
    nullif(trim(safe_cast(measure as string)), '') measure
from {{ set_datalake_project("world_oecd_piaac_staging.variable") }} as t
