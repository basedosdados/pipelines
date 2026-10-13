{{
    config(
        schema="au_abs_migration",
        alias="overseas_country_of_birth_australia",
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
    safe_cast(country_of_birth_id as string) country_of_birth_id,
    safe_cast(country_iso3_code as string) country_iso3_code,
    safe_cast(arrivals as int64) arrivals,
    safe_cast(departures as int64) departures,
    safe_cast(net as int64) net
from
    {{
        set_datalake_project(
            "au_abs_migration_staging.overseas_country_of_birth_australia"
        )
    }} as t
