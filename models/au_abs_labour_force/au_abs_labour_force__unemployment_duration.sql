{{
    config(
        schema="au_abs_labour_force",
        alias="unemployment_duration",
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
        cluster_by=["geography"],
    )
}}

select
    safe_cast(year as int64) year,
    safe_cast(month as int64) month,
    safe_cast(geography as string) geography,
    safe_cast(duration_band as string) duration_band,
    safe_cast(age_group as string) age_group,
    safe_cast(unemployed_total as float64) unemployed_total,
    safe_cast(
        unemployed_looked_for_full_time as float64
    ) unemployed_looked_for_full_time,
    safe_cast(
        unemployed_looked_for_part_time as float64
    ) unemployed_looked_for_part_time,
    safe_cast(weeks_searching as float64) weeks_searching
from
    {{ set_datalake_project("au_abs_labour_force_staging.unemployment_duration") }} as t
