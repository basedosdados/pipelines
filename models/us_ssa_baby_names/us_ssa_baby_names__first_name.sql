{{
    config(
        alias="first_name",
        schema="us_ssa_baby_names",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1880, "end": 2030, "interval": 1},
        },
    )
}}
select
    safe_cast(year as int64) year,
    safe_cast(sex as string) sex,
    safe_cast(name as string) name,
    safe_cast(count as int64) count
from {{ set_datalake_project("us_ssa_baby_names_staging.first_name") }} as t
