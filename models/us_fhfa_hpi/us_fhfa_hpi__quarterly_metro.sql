{{
    config(
        schema="us_fhfa_hpi",
        alias="quarterly_metro",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1975, "end": 2031, "interval": 1},
        },
        cluster_by=["cbsa_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(quarter as int64) quarter,
    safe_cast(cbsa_id as string) cbsa_id,
    safe_cast(cbsa_name as string) cbsa_name,
    safe_cast(index_type as string) index_type,
    safe_cast(index_flavor as string) index_flavor,
    safe_cast(index_nsa as float64) index_nsa,
    safe_cast(index_sa as float64) index_sa,
    safe_cast(relative_standard_error as float64) relative_standard_error,
    safe_cast(note as string) note
from {{ set_datalake_project("us_fhfa_hpi_staging.quarterly_metro") }} as t
