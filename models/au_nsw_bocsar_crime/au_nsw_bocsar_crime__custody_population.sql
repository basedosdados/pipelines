{{
    config(
        schema="au_nsw_bocsar_crime",
        alias="custody_population",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2031, "interval": 1},
        },
        cluster_by=["custody_system", "most_serious_offence"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(month as int64) month,
    safe_cast(custody_system as string) custody_system,
    safe_cast(legal_status as string) legal_status,
    safe_cast(aboriginality as string) aboriginality,
    safe_cast(sex as string) sex,
    safe_cast(most_serious_offence as string) most_serious_offence,
    safe_cast(people as int64) people
from {{ set_datalake_project("au_nsw_bocsar_crime_staging.custody_population") }} as t
