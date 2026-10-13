{{
    config(
        schema="world_iati_activities",
        alias="result_indicator",
        materialized="table",
    )
}}


select
    safe_cast(registry_dataset_id as string) registry_dataset_id,
    safe_cast(publisher_id as string) publisher_id,
    safe_cast(result_indicator_id as string) result_indicator_id,
    safe_cast(activity_id as string) activity_id,
    safe_cast(iati_identifier as string) iati_identifier,
    safe_cast(reporting_org_id as string) reporting_org_id,
    safe_cast(licence_id as string) licence_id,
    safe_cast(result_id as string) result_id,
    safe_cast(measure_code as string) measure_code,
    safe_cast(measure_name as string) measure_name,
    safe_cast(is_ascending as bool) is_ascending,
    safe_cast(is_aggregation_status as bool) is_aggregation_status,
    safe_cast(title as string) title,
    safe_cast(description as string) description
from {{ set_datalake_project("world_iati_activities_staging.result_indicator") }} as t
