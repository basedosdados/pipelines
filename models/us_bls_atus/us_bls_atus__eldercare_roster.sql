{{
    config(
        schema="us_bls_atus",
        alias="eldercare_roster",
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
    safe_cast(teage_ec as int64) teage_ec,
    safe_cast(teeldur as string) teeldur,
    safe_cast(teelwho as string) teelwho,
    safe_cast(teelyrs as int64) teelyrs,
    safe_cast(trelhh as string) trelhh,
    safe_cast(tueclno as string) tueclno,
    safe_cast(tulineno as string) tulineno
from {{ set_datalake_project("us_bls_atus_staging.eldercare_roster") }} as t
