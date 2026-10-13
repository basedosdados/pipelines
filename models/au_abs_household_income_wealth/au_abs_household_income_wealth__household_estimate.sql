{{
    config(
        schema="au_abs_household_income_wealth",
        alias="household_estimate",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2008, "end": 2025, "interval": 1},
        },
        cluster_by=["breakdown_type"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(survey_year as string) survey_year,
    safe_cast(geography as string) geography,
    safe_cast(geography_level as string) geography_level,
    safe_cast(source_table_id as string) source_table_id,
    safe_cast(source_table_name as string) source_table_name,
    safe_cast(breakdown_type as string) breakdown_type,
    safe_cast(breakdown_value as string) breakdown_value,
    safe_cast(measure as string) measure,
    safe_cast(measurement_unit as string) measurement_unit,
    safe_cast(estimate as float64) estimate,
    safe_cast(estimate_flag as string) estimate_flag,
    safe_cast(relative_standard_error as float64) relative_standard_error,
    safe_cast(margin_of_error as float64) margin_of_error
from
    {{
        set_datalake_project(
            "au_abs_household_income_wealth_staging.household_estimate"
        )
    }} as t
