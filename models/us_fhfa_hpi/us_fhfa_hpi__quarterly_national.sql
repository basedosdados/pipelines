{{
    config(
        schema="us_fhfa_hpi",
        alias="quarterly_national",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1975, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(quarter as int64) quarter,
    safe_cast(place_id as string) place_id,
    safe_cast(place_name as string) place_name,
    safe_cast(index_type as string) index_type,
    safe_cast(index_flavor as string) index_flavor,
    safe_cast(index_nsa as float64) index_nsa,
    safe_cast(index_sa as float64) index_sa
from {{ set_datalake_project("us_fhfa_hpi_staging.quarterly_national") }} as t
