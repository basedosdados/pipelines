{{
    config(
        schema="us_ed_ipeds",
        alias="eap",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2001, "end": 2030, "interval": 1},
        },
        cluster_by=["unitid"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(unitid as int64) unitid,
    safe_cast(eapcat as float64) eapcat,
    safe_cast(occupcat as float64) occupcat,
    safe_cast(facstat as float64) facstat,
    safe_cast(xeaptot as string) xeaptot,
    safe_cast(eaptot as float64) eaptot,
    safe_cast(xeaptyp as string) xeaptyp,
    safe_cast(eaptyp as float64) eaptyp,
    safe_cast(xeapmed as string) xeapmed,
    safe_cast(eapmed as float64) eapmed,
    safe_cast(xeapft as string) xeapft,
    safe_cast(eapft as float64) eapft,
    safe_cast(xeapftty as string) xeapftty,
    safe_cast(eapfttyp as float64) eapfttyp,
    safe_cast(xeapftmd as string) xeapftmd,
    safe_cast(eapftmed as float64) eapftmed,
    safe_cast(xeappt as string) xeappt,
    safe_cast(eappt as float64) eappt,
    safe_cast(xeapptty as string) xeapptty,
    safe_cast(eappttyp as float64) eappttyp,
    safe_cast(xeapptmd as string) xeapptmd,
    safe_cast(eapptmed as float64) eapptmed,
    safe_cast(eaprectp as float64) eaprectp,
    safe_cast(ftpt as float64) ftpt,
    safe_cast(functcd as float64) functcd,
    safe_cast(fstat as float64) fstat,
    safe_cast(typecd as float64) typecd,
    safe_cast(xfstat1 as string) xfstat1,
    safe_cast(fstat1 as float64) fstat1,
    safe_cast(xfstat2 as string) xfstat2,
    safe_cast(fstat2 as float64) fstat2,
    safe_cast(xfstat3 as string) xfstat3,
    safe_cast(fstat3 as float64) fstat3,
    safe_cast(xfstat4 as string) xfstat4,
    safe_cast(fstat4 as float64) fstat4,
    safe_cast(xfstat5 as string) xfstat5,
    safe_cast(fstat5 as float64) fstat5,
    safe_cast(xfstat6 as string) xfstat6,
    safe_cast(fstat6 as float64) fstat6
from
    {{ set_datalake_project("us_ed_ipeds_staging.eap") }} as t
    -- rematerialize us_ed_ipeds (table-approve re-trigger)
