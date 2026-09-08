{{
    config(
        schema="world_iati_activities",
        alias="activity",
        materialized="table",
    )
}}


select
    safe_cast(registry_dataset_id as string) registry_dataset_id,
    safe_cast(publisher_id as string) publisher_id,
    safe_cast(activity_id as string) activity_id,
    safe_cast(iati_identifier as string) iati_identifier,
    safe_cast(reporting_org_id as string) reporting_org_id,
    safe_cast(licence_id as string) licence_id,
    safe_cast(title as string) title,
    safe_cast(reporting_org_name as string) reporting_org_name,
    safe_cast(reporting_org_type_code as string) reporting_org_type_code,
    safe_cast(reporting_org_type_name as string) reporting_org_type_name,
    safe_cast(reporting_org_is_secondary as bool) reporting_org_is_secondary,
    safe_cast(activity_status_code as string) activity_status_code,
    safe_cast(activity_status_name as string) activity_status_name,
    safe_cast(activity_scope_code as string) activity_scope_code,
    safe_cast(activity_scope_name as string) activity_scope_name,
    safe_cast(planned_start_date as date) planned_start_date,
    safe_cast(actual_start_date as date) actual_start_date,
    safe_cast(planned_end_date as date) planned_end_date,
    safe_cast(actual_end_date as date) actual_end_date,
    safe_cast(collaboration_type_code as string) collaboration_type_code,
    safe_cast(collaboration_type_name as string) collaboration_type_name,
    safe_cast(default_flow_type_code as string) default_flow_type_code,
    safe_cast(default_flow_type_name as string) default_flow_type_name,
    safe_cast(default_finance_type_code as string) default_finance_type_code,
    safe_cast(default_finance_type_name as string) default_finance_type_name,
    safe_cast(default_tied_status_code as string) default_tied_status_code,
    safe_cast(default_tied_status_name as string) default_tied_status_name,
    safe_cast(default_currency_code as string) default_currency_code,
    safe_cast(default_currency_name as string) default_currency_name,
    safe_cast(capital_spend_percentage as float64) capital_spend_percentage,
    safe_cast(has_conditions_attached as bool) has_conditions_attached,
    safe_cast(is_humanitarian as bool) is_humanitarian,
    safe_cast(hierarchy_level as string) hierarchy_level,
    safe_cast(budget_not_provided_code as string) budget_not_provided_code,
    safe_cast(budget_not_provided_name as string) budget_not_provided_name,
    safe_cast(crs_channel_code as string) crs_channel_code,
    safe_cast(
        country_budget_items_vocabulary_code as string
    ) country_budget_items_vocabulary_code,
    safe_cast(
        country_budget_items_vocabulary_name as string
    ) country_budget_items_vocabulary_name,
    safe_cast(language_code as string) language_code,
    safe_cast(linked_data_uri as string) linked_data_uri,
    safe_cast(last_updated_datetime as datetime) last_updated_datetime
from {{ set_datalake_project("world_iati_activities_staging.activity") }} as t
