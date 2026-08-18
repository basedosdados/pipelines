{{
    config(
        schema="au_ato_taxation_statistics",
        alias="company_industry",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2016, "end": 2028, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(broad_industry_code as string) broad_industry_code,
    safe_cast(broad_industry as string) broad_industry,
    safe_cast(fine_industry_code as string) fine_industry_code,
    safe_cast(fine_industry as string) fine_industry,
    safe_cast(item as string) item,
    safe_cast(record_count as int64) record_count,
    safe_cast(amount as float64) amount
from
    {{ set_datalake_project("au_ato_taxation_statistics_staging.company_industry") }}
    as t
