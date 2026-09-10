{{
    config(
        schema="us_osha_enforcement",
        alias="violation_event",
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
    safe_cast(citation_id as string) citation_id,
    safe_cast(penalty_or_fta as string) penalty_or_fta,
    safe_cast(event_code as string) event_code,
    safe_cast(event_date as date) event_date,
    safe_cast(penalty as float64) penalty,
    safe_cast(abatement_date as date) abatement_date,
    safe_cast(violation_type as string) violation_type,
    safe_cast(fta_inspection_id as string) fta_inspection_id
from {{ set_datalake_project("us_osha_enforcement_staging.violation_event") }} as t
