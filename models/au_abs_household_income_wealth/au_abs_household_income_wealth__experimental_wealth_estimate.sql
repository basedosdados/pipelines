{{
    config(
        schema="au_abs_household_income_wealth",
        alias="experimental_wealth_estimate",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1994, "end": 2005, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(source_table_id as string) source_table_id,
    safe_cast(source_table_name as string) source_table_name,
    safe_cast(breakdown_type as string) breakdown_type,
    safe_cast(breakdown_value as string) breakdown_value,
    safe_cast(measure as string) measure,
    safe_cast(measurement_unit as string) measurement_unit,
    safe_cast(estimate as float64) estimate
from
    {{
        set_datalake_project(
            "au_abs_household_income_wealth_staging" ".experimental_wealth_estimate"
        )
    }} as t
