{{
    config(
        schema="in_tcpd_elections",
        alias="dictionary",
        materialized="table",
    )
}}


select
    safe_cast(table_id as string) table_id,
    safe_cast(column_name as string) column_name,
    safe_cast(key as string) key,
    safe_cast(temporal_coverage as string) temporal_coverage,
    safe_cast(value as string) value
from {{ set_datalake_project("in_tcpd_elections_staging.dictionary") }} as t
