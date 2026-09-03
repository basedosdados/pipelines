{{
    config(
        schema="au_doe_higher_education",
        alias="student_attrition_retention_success",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2014, "end": 2029, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(institution_id as string) institution_id,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(student_group as string) student_group,
    safe_cast(rate_basis as string) rate_basis,
    safe_cast(attrition_rate as float64) attrition_rate,
    safe_cast(retention_rate as float64) retention_rate,
    safe_cast(success_rate as float64) success_rate
from
    {{ set_datalake_project("au_doe_higher_education_staging.student_attrition_retention_success") }}
    as t
