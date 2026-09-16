{{
    config(
        schema="us_cms_open_payments",
        alias="summary_dashboard",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2019, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(dashboard_row_number as string) dashboard_row_number,
    safe_cast(metric as string) metric,
    safe_cast(value as float64) value
from {{ set_datalake_project("us_cms_open_payments_staging.summary_dashboard") }} as t
