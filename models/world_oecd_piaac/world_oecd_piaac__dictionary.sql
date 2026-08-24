{{
    config(
        alias="dictionary",
        schema="world_oecd_piaac",
        materialized="table",
    )
}}


select
    nullif(trim(safe_cast(table_id as string)), '') table_id,
    nullif(trim(safe_cast(column_name as string)), '') column_name,
    nullif(trim(safe_cast(key as string)), '') key,
    nullif(trim(safe_cast(temporal_coverage as string)), '') temporal_coverage,
    nullif(trim(safe_cast(value as string)), '') value
from {{ set_datalake_project("world_oecd_piaac_staging.dictionary") }} as t
