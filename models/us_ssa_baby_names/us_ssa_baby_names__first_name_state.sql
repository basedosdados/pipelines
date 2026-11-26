{{
    config(
        alias="first_name_state",
        schema="us_ssa_baby_names",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1910, "end": 2030, "interval": 1},
        },
    )
}}
select
    safe_cast(year as int64) year,
    safe_cast(state_id as string) state_id,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(sex as string) sex,
    safe_cast(name as string) name,
    safe_cast(count as int64) count
from {{ set_datalake_project("us_ssa_baby_names_staging.first_name_state") }} as t
