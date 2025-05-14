{{ config(alias="prefect_flows", schema="br_bd_metadados") }}
select
    safe_cast(project_name as string) project_name,
    safe_cast(flow_group_id as string) flow_group_id,
    safe_cast(name as string) name,
    datetime(left(flow_group_flows_aggregate_aggregate_min_created, 19)) created,
    safe_cast(version as int64) latest_version,
    datetime(left(created, 19)) last_update,
    safe_cast(schedule_type as string) schedule_type,
    safe_cast(schedule_cron as string) schedule_cron,
    datetime(trim(json_extract(schedule_start_date, '$.dt'), '"')) schedule_start_date,
    safe_cast(schedule_filters as string) schedule_filters,
    safe_cast(schedule_adjustments as string) schedule_adjustments,
    safe_cast(schedule_labels as string) schedule_labels,
    safe_cast(schedule_parameter_defaults as string) schedule_all_parameters,
    safe_cast(schedule_parameters_dataset_id as string) schedule_parameters_dataset_id,
    safe_cast(schedule_parameters_table_id as string) schedule_parameters_table_id,
    safe_cast(schedule_parameters_dbt_alias as bool) schedule_parameters_dbt_alias,
    safe_cast(
        schedule_parameters_materialization_mode as string
    ) schedule_parameters_materialization_mode,
    safe_cast(
        schedule_parameters_materialize_after_dump as bool
    ) schedule_parameters_materialize_after_dump,
    safe_cast(
        schedule_parameters_update_metadata as bool
    ) schedule_parameters_update_metadata,
from {{ set_datalake_project("br_bd_metadados_staging.prefect_flows") }} as t
