{{
    config(
        schema="au_abs_labour_force",
        alias="status_in_employment",
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
    safe_cast(status_in_employment as string) status_in_employment,
    safe_cast(employed_total as float64) employed_total,
    safe_cast(employed_full_time as float64) employed_full_time,
    safe_cast(employed_part_time as float64) employed_part_time
from {{ set_datalake_project("au_abs_labour_force_staging.status_in_employment") }} as t
