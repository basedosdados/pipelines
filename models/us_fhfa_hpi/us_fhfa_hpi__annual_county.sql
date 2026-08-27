{{
    config(
        schema="us_fhfa_hpi",
        alias="annual_county",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1975, "end": 2030, "interval": 1},
        },
        cluster_by=["county_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(county_id as string) county_id,
    safe_cast(county_name as string) county_name,
    safe_cast(annual_change_percent as float64) annual_change_percent,
    safe_cast(index_nsa as float64) index_nsa,
    safe_cast(index_nsa_1990_base as float64) index_nsa_1990_base,
    safe_cast(index_nsa_2000_base as float64) index_nsa_2000_base
from {{ set_datalake_project("us_fhfa_hpi_staging.annual_county") }} as t
