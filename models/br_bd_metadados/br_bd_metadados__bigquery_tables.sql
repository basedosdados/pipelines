{{ config(alias="bigquery_tables", schema="br_bd_metadados") }}
select
    project_id,
    dataset_id,
    table_id,
    case
        when type = '1'
        then 'table'
        when type = '2'
        then 'view'
        when type = '3'
        then 'external'
        else 'unknown'
    end as type,
    date(timestamp_millis(safe_cast(creation_time as int64))) as creation_date,
    date(timestamp_millis(safe_cast(last_modified_time as int64))) as last_modified_date
    ,
    timestamp_millis(safe_cast(creation_time as int64)) as creation_time,
    timestamp_millis(safe_cast(last_modified_time as int64)) as last_modified_time,
    safe_cast(row_count as int64) as row_count,
    safe_cast(size_bytes as int64) as size_bytes
from {{ set_datalake_project("br_bd_metadados_staging.bigquery_tables") }}
