{{
    config(
        schema="us_bls_atus",
        alias="case_history",
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
    safe_cast(tr1intst as string) tr1intst,
    safe_cast(tr2intst as string) tr2intst,
    safe_cast(trfnlout as string) trfnlout,
    safe_cast(trincen2 as string) trincen2,
    safe_cast(tuavgdur as float64) tuavgdur,
    safe_cast(tua_id as string) tua_id,
    safe_cast(tucpsdp as string) tucpsdp,
    safe_cast(tuc_id as string) tuc_id,
    safe_cast(tudqual2 as string) tudqual2,
    safe_cast(tuincent as string) tuincent,
    safe_cast(tuintdqual as string) tuintdqual,
    safe_cast(tuintid as string) tuintid,
    safe_cast(tuintrodate as string) tuintrodate,
    safe_cast(tuintropanmonth as int64) tuintropanmonth,
    safe_cast(tuintropanyear as int64) tuintropanyear,
    safe_cast(tulngskl as string) tulngskl,
    safe_cast(tutotactno as int64) tutotactno,
    safe_cast(tuv_id as string) tuv_id
from {{ set_datalake_project("us_bls_atus_staging.case_history") }} as t
