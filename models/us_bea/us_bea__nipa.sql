{{
    config(
        schema="us_bea",
        alias="nipa",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1920, "end": 2035, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(quarter as int64) quarter,
    safe_cast(month as int64) month,
    safe_cast(frequency as string) frequency,
    safe_cast(table_name as string) table_id,
    safe_cast(line_number as string) line_number,
    safe_cast(series_code as string) series_id,
    safe_cast(line_description as string) line_description,
    safe_cast(metric_name as string) metric_name,
    safe_cast(unit as string) unit,
    safe_cast(unit_mult as string) unit_mult,
    safe_cast(value as float64) value,
    safe_cast(note_ref as string) note_ref
from {{ set_datalake_project("us_bea_staging.nipa") }} as t
