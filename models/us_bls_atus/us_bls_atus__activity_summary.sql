{{
    config(
        schema="us_bls_atus",
        alias="activity_summary",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2003, "end": 2029, "interval": 1},
        },
        cluster_by=["tucaseid"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(tucaseid as string) tucaseid,
    safe_cast(activity_code as string) activity_code,
    safe_cast(duration_minutes as int64) duration_minutes
from {{ set_datalake_project("us_bls_atus_staging.activity_summary") }} as t
