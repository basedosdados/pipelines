{{
    config(
        schema="au_rba_statistical_tables",
        alias="data",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1922, "end": 2031, "interval": 1},
        },
        cluster_by=["table_id", "series_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(date as date) date,
    safe_cast(table_id as string) table_id,
    safe_cast(series_id as string) series_id,
    safe_cast(value as float64) value
from {{ set_datalake_project("au_rba_statistical_tables_staging.data") }} as t
