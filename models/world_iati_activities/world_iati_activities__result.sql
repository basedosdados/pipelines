{{
    config(
        schema="world_iati_activities",
        alias="result",
        materialized="table",
    )
}}


select
    safe_cast(registry_dataset_id as string) registry_dataset_id,
    safe_cast(publisher_id as string) publisher_id,
    safe_cast(result_id as string) result_id,
    safe_cast(activity_id as string) activity_id,
    safe_cast(iati_identifier as string) iati_identifier,
    safe_cast(reporting_org_id as string) reporting_org_id,
    safe_cast(licence_id as string) licence_id,
    safe_cast(result_type_code as string) result_type_code,
    safe_cast(result_type_name as string) result_type_name,
    safe_cast(is_aggregation_status as bool) is_aggregation_status,
    safe_cast(title as string) title,
    safe_cast(description as string) description
from {{ set_datalake_project("world_iati_activities_staging.result") }} as t
