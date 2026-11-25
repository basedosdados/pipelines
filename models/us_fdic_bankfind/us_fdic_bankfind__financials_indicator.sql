{{
    config(
        schema="us_fdic_bankfind",
        alias="financials_indicator",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1984, "end": 2031, "interval": 1},
        },
        cluster_by=["indicator_id", "cert"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(quarter as int64) quarter,
    safe_cast(report_date as date) report_date,
    safe_cast(cert as string) cert,
    safe_cast(indicator_id as string) indicator_id,
    safe_cast(value as float64) value
from {{ set_datalake_project("us_fdic_bankfind_staging.financials_indicator") }} as t
