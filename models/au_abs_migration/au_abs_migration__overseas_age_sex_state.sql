{{
    config(
        schema="au_abs_migration",
        alias="overseas_age_sex_state",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2004, "end": 2029, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(state_id as string) state_id,
    safe_cast(age_group as string) age_group,
    safe_cast(sex as string) sex,
    safe_cast(arrivals as int64) arrivals,
    safe_cast(departures as int64) departures,
    safe_cast(net as int64) net
from {{ set_datalake_project("au_abs_migration_staging.overseas_age_sex_state") }} as t
