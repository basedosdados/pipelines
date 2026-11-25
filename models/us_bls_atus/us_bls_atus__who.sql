{{
    config(
        schema="us_bls_atus",
        alias="who",
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
    safe_cast(tulineno as string) tulineno,
    safe_cast(tuactivity_n as string) tuactivity_n,
    safe_cast(trwhona as string) trwhona,
    safe_cast(tuwho_code as string) tuwho_code
from {{ set_datalake_project("us_bls_atus_staging.who") }} as t
