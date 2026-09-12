{{
    config(
        schema="world_iati_activities",
        alias="policy_marker",
        materialized="table",
    )
}}


select
    safe_cast(registry_dataset_id as string) registry_dataset_id,
    safe_cast(publisher_id as string) publisher_id,
    safe_cast(policy_marker_id as string) policy_marker_id,
    safe_cast(activity_id as string) activity_id,
    safe_cast(iati_identifier as string) iati_identifier,
    safe_cast(reporting_org_id as string) reporting_org_id,
    safe_cast(licence_id as string) licence_id,
    safe_cast(vocabulary_code as string) vocabulary_code,
    safe_cast(vocabulary_name as string) vocabulary_name,
    safe_cast(vocabulary_uri as string) vocabulary_uri,
    safe_cast(policy_marker_code as string) policy_marker_code,
    safe_cast(policy_marker_name as string) policy_marker_name,
    safe_cast(significance_code as string) significance_code,
    safe_cast(significance_name as string) significance_name,
    safe_cast(policy_marker_narrative as string) policy_marker_narrative
from {{ set_datalake_project("world_iati_activities_staging.policy_marker") }} as t
