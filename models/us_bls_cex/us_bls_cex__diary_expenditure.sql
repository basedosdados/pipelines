{{
    config(
        schema="us_bls_cex",
        alias="diary_expenditure",
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
    safe_cast(alloc as string) alloc,
    safe_cast(cost as float64) cost,
    safe_cast(pub_flag as string) pub_flag,
    safe_cast(ucc as string) ucc,
    safe_cast(expnsqdy as string) expnsqdy,
    safe_cast(expnwkdy as string) expnwkdy,
    safe_cast(expnmo as string) expnmo,
    safe_cast(expnmo_ as string) expnmo_,
    safe_cast(expnyr as int64) expnyr,
    safe_cast(expnyr_ as string) expnyr_,
    safe_cast(gift as string) gift,
    safe_cast(qredate as string) qredate,
    safe_cast(qredate_ as string) qredate_,
    safe_cast(expn_qdy as string) expn_qdy,
    safe_cast(expn_kdy as string) expn_kdy
from {{ set_datalake_project("us_bls_cex_staging.diary_expenditure") }} as t
