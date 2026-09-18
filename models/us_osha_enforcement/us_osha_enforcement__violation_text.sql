{{
    config(
        schema="us_osha_enforcement",
        alias="violation_text",
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
    safe_cast(text as string) text,
    safe_cast(line_count as int64) line_count
from {{ set_datalake_project("us_osha_enforcement_staging.violation_text") }} as t
