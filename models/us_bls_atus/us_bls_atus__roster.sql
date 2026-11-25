{{
    config(
        schema="us_bls_atus",
        alias="roster",
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
    safe_cast(terrp as string) terrp,
    safe_cast(teage as int64) teage,
    safe_cast(tesex as string) tesex
from {{ set_datalake_project("us_bls_atus_staging.roster") }} as t
