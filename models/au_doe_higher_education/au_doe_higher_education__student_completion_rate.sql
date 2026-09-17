{{
    config(
        schema="au_doe_higher_education",
        alias="student_completion_rate",
        materialized="table",
        partition_by={
            "field": "cohort_start_year",
            "data_type": "int64",
            "range": {"start": 2007, "end": 2026, "interval": 1},
        },
    )
}}


select
    safe_cast(cohort_start_year as int64) cohort_start_year,
    safe_cast(cohort_end_year as int64) cohort_end_year,
    safe_cast(tracking_years as int64) tracking_years,
    safe_cast(institution_id as string) institution_id,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(provider_group as string) provider_group,
    safe_cast(dimension as string) dimension,
    safe_cast(dimension_value as string) dimension_value,
    safe_cast(completed_rate as float64) completed_rate,
    safe_cast(still_enrolled_rate as float64) still_enrolled_rate,
    safe_cast(re_enrolled_dropped_out_rate as float64) re_enrolled_dropped_out_rate,
    safe_cast(never_returned_rate as float64) never_returned_rate
from
    {{
        set_datalake_project(
            "au_doe_higher_education_staging.student_completion_rate"
        )
    }} as t
