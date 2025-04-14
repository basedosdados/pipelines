{{ config(alias="prefect_flow_runs", schema="br_bd_metadados") }}
select
    safe_cast(id as string) id,
    safe_cast(flow_flow_group_id as string) flow_group_id,
    safe_cast(name as string) name,
    safe_cast(labels as string) labels,
    safe_cast(flow_project_name as string) flow_project_name,
    safe_cast(flow_name as string) flow_name,
    safe_cast(flow_archived as bool) flow_archived,
    safe_cast(dataset_id as string) dataset_id,
    safe_cast(table_id as string) table_id,
    datetime(left(start_time, 19)) start_time,
    datetime(left(end_time, 19)) end_time,
    safe_cast(state as string) state,
    safe_cast(state_message as string) state_message,
    safe_cast(task_runs as string) task_runs,
    safe_cast(skipped_upload_to_gcs as bool) skipped_upload_to_gcs,
    safe_cast(logs as string) error_logs,
from {{ set_datalake_project("br_bd_metadados_staging.prefect_flow_runs") }} as t
