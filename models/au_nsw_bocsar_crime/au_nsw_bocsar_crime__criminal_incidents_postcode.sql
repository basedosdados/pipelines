{{
    config(
        schema="au_nsw_bocsar_crime",
        alias="criminal_incidents_postcode",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1995, "end": 2031, "interval": 1},
        },
        cluster_by=["postcode", "offence_category"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(month as int64) month,
    safe_cast(postcode as string) postcode,
    safe_cast(offence_category as string) offence_category,
    safe_cast(offence_subcategory as string) offence_subcategory,
    safe_cast(incidents as int64) incidents
from
    {{
        set_datalake_project(
            "au_nsw_bocsar_crime_staging.criminal_incidents_postcode"
        )
    }} as t
