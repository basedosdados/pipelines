{{
    config(
        schema="au_doe_higher_education",
        alias="student_load",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2016, "end": 2029, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(institution_id as string) institution_id,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(citizenship as string) citizenship,
    safe_cast(commencing as string) commencing,
    safe_cast(course_level_broad as string) course_level_broad,
    safe_cast(course_level_detailed as string) course_level_detailed,
    safe_cast(discipline as string) discipline,
    safe_cast(gender as string) gender,
    safe_cast(liability_status as string) liability_status,
    safe_cast(student_load_eftsl as float64) student_load_eftsl
from
    {{ set_datalake_project("au_doe_higher_education_staging.student_load") }}
    as t
