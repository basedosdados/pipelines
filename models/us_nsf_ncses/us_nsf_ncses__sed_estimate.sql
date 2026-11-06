{{
    config(
        schema="us_nsf_ncses",
        alias="sed_estimate",
        materialized="table",
        partition_by={
            "field": "reference_year",
            "data_type": "int64",
            "range": {"start": 2024, "end": 2029, "interval": 1},
        },
    )
}}


select
    safe_cast(reference_year as int64) reference_year,
    safe_cast(table_id as string) table_id,
    safe_cast(year as int64) year,
    safe_cast(row_number as string) row_number,
    safe_cast(column_number as string) column_number,
    safe_cast(row_label as string) row_label,
    safe_cast(row_path as string) row_path,
    safe_cast(row_level as string) row_level,
    safe_cast(column_label as string) column_label,
    safe_cast(column_path as string) column_path,
    safe_cast(unit as string) unit,
    safe_cast(value as float64) value
from {{ set_datalake_project("us_nsf_ncses_staging.sed_estimate") }} as t
