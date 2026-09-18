{{
    config(
        schema="au_abs_labour_force",
        alias="employment_occupation",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {
                "start": 1984,
                "end": 2031,
                "interval": 1,
            },
        },
        cluster_by=["occupation_sub_major"],
    )
}}

select
    safe_cast(year as int64) year,
    safe_cast(quarter as int64) quarter,
    safe_cast(month as int64) month,
    safe_cast(occupation_sub_major as string) occupation_sub_major,
    safe_cast(occupation_classification as string) occupation_classification,
    safe_cast(sex as string) sex,
    safe_cast(age_group as string) age_group,
    safe_cast(employed_full_time as float64) employed_full_time,
    safe_cast(employed_part_time as float64) employed_part_time,
    safe_cast(hours_worked_full_time as float64) hours_worked_full_time,
    safe_cast(hours_worked_part_time as float64) hours_worked_part_time
from
    {{ set_datalake_project("au_abs_labour_force_staging.employment_occupation") }} as t
