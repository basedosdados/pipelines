{{
    config(
        alias="dictionary",
        schema="world_oecd_piaac",
        materialized="table",
    )
}}


select
    nullif(nullif(trim(safe_cast(table_id as string)), ''), '.') table_id,
    nullif(nullif(trim(safe_cast(column_name as string)), ''), '.') column_name,
    nullif(nullif(trim(safe_cast(key as string)), ''), '.') key,
    nullif(
        nullif(trim(safe_cast(temporal_coverage as string)), ''), '.'
    ) temporal_coverage,
    nullif(nullif(trim(safe_cast(value as string)), ''), '.') value
from {{ set_datalake_project("world_oecd_piaac_staging.dictionary") }} as t
