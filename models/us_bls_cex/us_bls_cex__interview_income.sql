{{
    config(
        schema="us_bls_cex",
        alias="interview_income",
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
    safe_cast(ucc as string) ucc,
    safe_cast(pubflag as string) pubflag,
    safe_cast(value as float64) value,
    safe_cast(value_ as string) value_,
    safe_cast(gift as string) gift
from {{ set_datalake_project("us_bls_cex_staging.interview_income") }} as t
