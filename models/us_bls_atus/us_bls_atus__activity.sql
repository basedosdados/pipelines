{{
    config(
        schema="us_bls_atus",
        alias="activity",
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
    safe_cast(tuactivity_n as string) tuactivity_n,
    safe_cast(tuactdur24 as int64) tuactdur24,
    safe_cast(tucc5 as string) tucc5,
    safe_cast(tucc5b as string) tucc5b,
    safe_cast(trtcctot_ln as int64) trtcctot_ln,
    safe_cast(trtcc_ln as int64) trtcc_ln,
    safe_cast(trtcoc_ln as int64) trtcoc_ln,
    safe_cast(tustarttim as string) tustarttim,
    safe_cast(tustoptime as string) tustoptime,
    safe_cast(trcodep as string) trcodep,
    safe_cast(trtier1p as string) trtier1p,
    safe_cast(trtier2p as string) trtier2p,
    safe_cast(tucc8 as string) tucc8,
    safe_cast(tucumdur as int64) tucumdur,
    safe_cast(tucumdur24 as int64) tucumdur24,
    safe_cast(tuactdur as int64) tuactdur,
    safe_cast(tr_03cc57 as string) tr_03cc57,
    safe_cast(trto_ln as int64) trto_ln,
    safe_cast(trtonhh_ln as int64) trtonhh_ln,
    safe_cast(trtohh_ln as int64) trtohh_ln,
    safe_cast(trthh_ln as int64) trthh_ln,
    safe_cast(trtnohh_ln as int64) trtnohh_ln,
    safe_cast(tewhere as string) tewhere,
    safe_cast(tucc7 as string) tucc7,
    safe_cast(trwbelig as string) trwbelig,
    safe_cast(trtec_ln as int64) trtec_ln,
    safe_cast(tuec24 as string) tuec24,
    safe_cast(tudurstop as string) tudurstop
from {{ set_datalake_project("us_bls_atus_staging.activity") }} as t
