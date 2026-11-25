{{
    config(
        schema="au_abs_labour_force",
        alias="labour_force_status",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1978, "end": 2031, "interval": 1},
        },
        cluster_by=["geography", "adjustment_type"],
    )
}}

select
    safe_cast(year as int64) year,
    safe_cast(month as int64) month,
    safe_cast(geography as string) geography,
    safe_cast(sex as string) sex,
    safe_cast(age_group as string) age_group,
    safe_cast(adjustment_type as string) adjustment_type,
    safe_cast(employed_total as float64) employed_total,
    safe_cast(employed_full_time as float64) employed_full_time,
    safe_cast(employed_part_time as float64) employed_part_time,
    safe_cast(unemployed_total as float64) unemployed_total,
    safe_cast(
        unemployed_looked_for_full_time as float64
    ) unemployed_looked_for_full_time,
    safe_cast(
        unemployed_looked_for_part_time as float64
    ) unemployed_looked_for_part_time,
    safe_cast(labour_force_total as float64) labour_force_total,
    safe_cast(not_in_labour_force as float64) not_in_labour_force,
    safe_cast(civilian_population_15_over as float64) civilian_population_15_over,
    safe_cast(unemployment_rate as float64) unemployment_rate,
    safe_cast(
        unemployment_rate_looked_for_full_time as float64
    ) unemployment_rate_looked_for_full_time,
    safe_cast(
        unemployment_rate_looked_for_part_time as float64
    ) unemployment_rate_looked_for_part_time,
    safe_cast(participation_rate as float64) participation_rate,
    safe_cast(employment_to_population_ratio as float64) employment_to_population_ratio
from {{ set_datalake_project("au_abs_labour_force_staging.labour_force_status") }} as t
