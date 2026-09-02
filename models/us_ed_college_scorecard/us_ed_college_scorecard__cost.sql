{{
    config(
        schema="us_ed_college_scorecard",
        alias="cost",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1996, "end": 2030, "interval": 1},
        },
        cluster_by=["variable_name"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(unitid as string) unitid,
    safe_cast(variable_name as string) variable_name,
    safe_cast(value as float64) value,
    safe_cast(value_raw as string) value_raw
from {{ set_datalake_project("us_ed_college_scorecard_staging.cost") }} as t
