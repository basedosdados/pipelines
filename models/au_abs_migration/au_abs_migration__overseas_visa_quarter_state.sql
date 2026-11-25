{{
    config(
        schema="au_abs_migration",
        alias="overseas_visa_quarter_state",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2006, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(quarter as int64) quarter,
    safe_cast(state_id as string) state_id,
    safe_cast(visa_group_id as string) visa_group_id,
    safe_cast(arrivals as int64) arrivals,
    safe_cast(departures as int64) departures
from
    {{ set_datalake_project("au_abs_migration_staging.overseas_visa_quarter_state") }}
    as t
