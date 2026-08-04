{{
    config(
        schema="au_abs_cpi",
        alias="quarterly",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1948, "end": 2031, "interval": 1},
        },
        cluster_by=["region", "index_name"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(quarter as int64) quarter,
    safe_cast(region as string) region,
    safe_cast(serie_id as string) serie_id,
    safe_cast(index_name as string) index_name,
    safe_cast(index_number as float64) index_number,
    safe_cast(percentage_change_period as float64) percentage_change_period,
    safe_cast(percentage_change_year as float64) percentage_change_year
from {{ set_datalake_project("au_abs_cpi_staging.quarterly") }} as t
