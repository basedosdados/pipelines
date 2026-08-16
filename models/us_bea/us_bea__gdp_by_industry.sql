{{
    config(
        schema="us_bea",
        alias="gdp_by_industry",
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
    safe_cast(frequency as string) frequency,
    safe_cast(table_id as string) table_id,
    safe_cast(table_description as string) table_description,
    safe_cast(industry as string) industry,
    safe_cast(industry_description as string) industry_description,
    safe_cast(value as float64) value,
    safe_cast(note_ref as string) note_ref
from {{ set_datalake_project("us_bea_staging.gdp_by_industry") }} as t
