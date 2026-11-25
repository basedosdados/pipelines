{{
    config(
        schema="au_nsw_bocsar_crime",
        alias="custody_receptions",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2031, "interval": 1},
        },
        cluster_by=["custody_system"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(month as int64) month,
    safe_cast(custody_system as string) custody_system,
    safe_cast(reception_status as string) reception_status,
    safe_cast(aboriginality as string) aboriginality,
    safe_cast(sex as string) sex,
    safe_cast(receptions as int64) receptions
from {{ set_datalake_project("au_nsw_bocsar_crime_staging.custody_receptions") }} as t
