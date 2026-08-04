{{
    config(
        schema="au_nsw_bocsar_crime",
        alias="custody_discharges",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2031, "interval": 1},
        },
        cluster_by=["custody_system", "discharge_type"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(month as int64) month,
    safe_cast(custody_system as string) custody_system,
    safe_cast(discharge_type as string) discharge_type,
    safe_cast(discharge_type_breakdown as string) discharge_type_breakdown,
    safe_cast(aboriginality as string) aboriginality,
    safe_cast(sex as string) sex,
    safe_cast(discharges as int64) discharges
from {{ set_datalake_project("au_nsw_bocsar_crime_staging.custody_discharges") }} as t
