{{
    config(
        alias="votes",
        schema="us_ucla_voteview",
        materialized="table",
        partition_by={
            "field": "congress",
            "data_type": "int64",
            "range": {"start": 1, "end": 130, "interval": 1},
        },
    )
}}
select
    safe_cast(congress as int64) congress,
    safe_cast(chamber as string) chamber,
    safe_cast(rollnumber as string) rollnumber,
    safe_cast(date as date) date,
    safe_cast(icpsr as string) icpsr,
    safe_cast(cast_code as string) cast_code,
    safe_cast(prob as float64) prob
from {{ set_datalake_project("us_ucla_voteview_staging.votes") }} as t
