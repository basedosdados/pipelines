{{
    config(
        schema="world_iati_activities",
        alias="organisation",
        materialized="table",
    )
}}


select
    safe_cast(registry_dataset_id as string) registry_dataset_id,
    safe_cast(publisher_id as string) publisher_id,
    safe_cast(organisation_id as string) organisation_id,
    safe_cast(organisation_identifier as string) organisation_identifier,
    safe_cast(reporting_org_id as string) reporting_org_id,
    safe_cast(licence_id as string) licence_id,
    safe_cast(name as string) name,
    safe_cast(reporting_org_name as string) reporting_org_name,
    safe_cast(reporting_org_type_code as string) reporting_org_type_code,
    safe_cast(reporting_org_type_name as string) reporting_org_type_name,
    safe_cast(reporting_org_is_secondary as bool) reporting_org_is_secondary,
    safe_cast(default_currency_code as string) default_currency_code,
    safe_cast(default_currency_name as string) default_currency_name,
    safe_cast(language_code as string) language_code,
    safe_cast(last_updated_datetime as datetime) last_updated_datetime
from {{ set_datalake_project("world_iati_activities_staging.organisation") }} as t
