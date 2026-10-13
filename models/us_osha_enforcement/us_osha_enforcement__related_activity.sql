{{
    config(
        schema="us_osha_enforcement",
        alias="related_activity",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1970, "end": 2035, "interval": 1},
        },
        cluster_by=["inspection_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(inspection_id as string) inspection_id,
    safe_cast(related_activity_id as string) related_activity_id,
    safe_cast(related_type as string) related_type,
    safe_cast(related_safety as string) related_safety,
    safe_cast(related_health as string) related_health
from {{ set_datalake_project("us_osha_enforcement_staging.related_activity") }} as t
