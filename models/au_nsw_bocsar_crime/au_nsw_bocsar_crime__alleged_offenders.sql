{{
    config(
        schema="au_nsw_bocsar_crime",
        alias="alleged_offenders",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2010, "end": 2031, "interval": 1},
        },
        cluster_by=["offence_category"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(financial_year as string) financial_year,
    safe_cast(offence_category as string) offence_category,
    safe_cast(offence_subcategory as string) offence_subcategory,
    safe_cast(age_group as string) age_group,
    safe_cast(legal_proceeding as string) legal_proceeding,
    safe_cast(detailed_legal_proceeding as string) detailed_legal_proceeding,
    safe_cast(poi_count as int64) poi_count
from {{ set_datalake_project("au_nsw_bocsar_crime_staging.alleged_offenders") }} as t
