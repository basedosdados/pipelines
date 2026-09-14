{{
    config(
        schema="us_ffiec_bank_reporting",
        alias="call_report_item",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2009, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(quarter as int64) quarter,
    safe_cast(report_date as date) report_date,
    safe_cast(rssd_id as string) rssd_id,
    safe_cast(schedule as string) schedule,
    safe_cast(item_code as string) item_code,
    safe_cast(value as float64) value
from
    {{ set_datalake_project("us_ffiec_bank_reporting_staging.call_report_item") }}
    as t
