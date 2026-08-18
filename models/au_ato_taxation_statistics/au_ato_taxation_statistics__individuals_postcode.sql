{{
    config(
        schema="au_ato_taxation_statistics",
        alias="individuals_postcode",
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
    safe_cast(taxable_status as string) taxable_status,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(sa4_name as string) sa4_name,
    safe_cast(postcode as string) postcode,
    safe_cast(item as string) item,
    safe_cast(record_count as int64) record_count,
    safe_cast(amount as float64) amount
from
    {{
        set_datalake_project(
            "au_ato_taxation_statistics_staging.individuals_postcode"
        )
    }} as t
