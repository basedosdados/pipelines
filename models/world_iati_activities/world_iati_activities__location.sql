{{
    config(
        schema="world_iati_activities",
        alias="location",
        materialized="table",
    )
}}


select
    safe_cast(registry_dataset_id as string) registry_dataset_id,
    safe_cast(publisher_id as string) publisher_id,
    safe_cast(location_id as string) location_id,
    safe_cast(activity_id as string) activity_id,
    safe_cast(iati_identifier as string) iati_identifier,
    safe_cast(reporting_org_id as string) reporting_org_id,
    safe_cast(licence_id as string) licence_id,
    safe_cast(location_ref as string) location_ref,
    safe_cast(name as string) name,
    safe_cast(description as string) description,
    safe_cast(activity_description as string) activity_description,
    safe_cast(point_position as string) point_position,
    safe_cast(point_srs_name as string) point_srs_name,
    safe_cast(location_reach_code as string) location_reach_code,
    safe_cast(location_reach_name as string) location_reach_name,
    safe_cast(exactness_code as string) exactness_code,
    safe_cast(exactness_name as string) exactness_name,
    safe_cast(location_class_code as string) location_class_code,
    safe_cast(location_class_name as string) location_class_name,
    safe_cast(feature_designation_code as string) feature_designation_code,
    safe_cast(feature_designation_name as string) feature_designation_name
from {{ set_datalake_project("world_iati_activities_staging.location") }} as t
