{{
    config(
        schema="world_iati_activities",
        alias="result_indicator_period",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 0, "end": 2043, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(registry_dataset_id as string) registry_dataset_id,
    safe_cast(publisher_id as string) publisher_id,
    safe_cast(result_indicator_period_id as string) result_indicator_period_id,
    safe_cast(activity_id as string) activity_id,
    safe_cast(iati_identifier as string) iati_identifier,
    safe_cast(reporting_org_id as string) reporting_org_id,
    safe_cast(licence_id as string) licence_id,
    safe_cast(result_id as string) result_id,
    safe_cast(result_indicator_id as string) result_indicator_id,
    safe_cast(period_start_date as date) period_start_date,
    safe_cast(period_end_date as date) period_end_date,
    safe_cast(target_value as string) target_value,
    safe_cast(actual_value as string) actual_value
from
    {{ set_datalake_project("world_iati_activities_staging.result_indicator_period") }}
    as t
