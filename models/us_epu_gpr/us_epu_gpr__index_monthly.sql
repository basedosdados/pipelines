{{
    config(
        schema="us_epu_gpr",
        alias="index_monthly",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1900, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(month as int64) month,
    safe_cast(country_id as string) country_id,
    safe_cast(index_family as string) index_family,
    safe_cast(index_name as string) index_name,
    safe_cast(value as float64) value
from {{ set_datalake_project("us_epu_gpr_staging.index_monthly") }} as t
