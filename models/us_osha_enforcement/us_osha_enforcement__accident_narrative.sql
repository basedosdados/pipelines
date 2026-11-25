{{
    config(
        schema="us_osha_enforcement",
        alias="accident_narrative",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1970, "end": 2035, "interval": 1},
        },
        cluster_by=["accident_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(accident_id as string) accident_id,
    safe_cast(narrative as string) narrative,
    safe_cast(line_count as int64) line_count,
    safe_cast(wrap_style as string) wrap_style
from {{ set_datalake_project("us_osha_enforcement_staging.accident_narrative") }} as t
