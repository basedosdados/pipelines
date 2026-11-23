{{
    config(
        schema="us_bls_cex",
        alias="diary_income",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1996, "end": 2031, "interval": 1},
        },
        cluster_by=["ucc"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(quarter as int64) quarter,
    safe_cast(consumer_unit_id as string) consumer_unit_id,
    safe_cast(diary_week as string) diary_week,
    safe_cast(newid as string) newid,
    safe_cast(ucc as string) ucc,
    safe_cast(amount as float64) amount,
    safe_cast(amount_ as string) amount_,
    safe_cast(pub_flag as string) pub_flag
from {{ set_datalake_project("us_bls_cex_staging.diary_income") }} as t
