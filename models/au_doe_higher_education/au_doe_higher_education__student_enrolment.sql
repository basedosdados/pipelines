{{
    config(
        schema="au_doe_higher_education",
        alias="student_enrolment",
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
    safe_cast(gender as string) gender,
    safe_cast(attendance_mode as string) attendance_mode,
    safe_cast(attendance_type as string) attendance_type,
    safe_cast(special_course as string) special_course,
    safe_cast(field_of_education_primary as string) field_of_education_primary,
    safe_cast(field_of_education_secondary as string) field_of_education_secondary,
    safe_cast(enrolments as int64) enrolments
from
    {{ set_datalake_project("au_doe_higher_education_staging.student_enrolment") }} as t
