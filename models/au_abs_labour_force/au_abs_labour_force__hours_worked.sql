{{
    config(
        schema="au_abs_labour_force",
        alias="hours_worked",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1991, "end": 2031, "interval": 1},
        },
        cluster_by=["geography"],
    )
}}

select
    safe_cast(year as int64) year,
    safe_cast(month as int64) month,
    safe_cast(geography as string) geography,
    safe_cast(sex as string) sex,
    safe_cast(hours_band as string) hours_band,
    safe_cast(employed_persons as float64) employed_persons,
    safe_cast(hours_worked as float64) hours_worked,
    safe_cast(hours_per_person as float64) hours_per_person
from {{ set_datalake_project("au_abs_labour_force_staging.hours_worked") }} as t
