{{
    config(
        schema="au_abs_labour_force",
        alias="underutilisation",
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
    safe_cast(underemployed_total as float64) underemployed_total,
    safe_cast(underemployment_ratio as float64) underemployment_ratio,
    safe_cast(underemployment_rate as float64) underemployment_rate,
    safe_cast(underutilisation_rate as float64) underutilisation_rate
from {{ set_datalake_project("au_abs_labour_force_staging.underutilisation") }} as t
