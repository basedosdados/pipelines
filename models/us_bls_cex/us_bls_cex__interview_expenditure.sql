{{
    config(
        schema="us_bls_cex",
        alias="interview_expenditure",
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
    safe_cast(interview_number as string) interview_number,
    safe_cast(newid as string) newid,
    safe_cast(reference_month as int64) reference_month,
    safe_cast(reference_year as int64) reference_year,
    safe_cast(seqno as string) seqno,
    safe_cast(alcno as string) alcno,
    safe_cast(expname as string) expname,
    safe_cast(rtype as string) rtype,
    safe_cast(gift as string) gift,
    safe_cast(uccseq as string) uccseq,
    safe_cast(ucc as string) ucc,
    safe_cast(cost as float64) cost,
    safe_cast(cost_ as string) cost_,
    safe_cast(pubflag as string) pubflag
from {{ set_datalake_project("us_bls_cex_staging.interview_expenditure") }} as t
