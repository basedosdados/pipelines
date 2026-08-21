{{
    config(
        alias="variable",
        schema="world_oecd_piaac",
        materialized="table",
    )
}}


select
    safe_cast(cycle as string) cycle,
    safe_cast(variable_name as string) variable_name,
    safe_cast(table_id as string) table_id,
    safe_cast(column_name as string) column_name,
    safe_cast(label as string) label,
    safe_cast(domain as string) domain,
    safe_cast(level as string) level,
    safe_cast(bigquery_type as string) bigquery_type,
    safe_cast(measurement_unit as string) measurement_unit,
    safe_cast(item_code as string) item_code,
    safe_cast(measure as string) measure
from {{ set_datalake_project("world_oecd_piaac_staging.variable") }} as t
