{{
    config(
        schema="us_nsf_ncses",
        alias="herd_institution",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1972, "end": 2029, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(institution_id as string) institution_id,
    safe_cast(ncses_institution_id as string) ncses_institution_id,
    safe_cast(unitid as string) unitid,
    safe_cast(combined_institution_id as string) combined_institution_id,
    safe_cast(survey_form as string) survey_form,
    safe_cast(institution_name as string) institution_name,
    safe_cast(institution_city as string) institution_city,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(zip_code as string) zip_code,
    safe_cast(hbcu_indicator as string) hbcu_indicator,
    safe_cast(medical_school_indicator as string) medical_school_indicator,
    safe_cast(
        high_hispanic_enrollment_indicator as string
    ) high_hispanic_enrollment_indicator,
    safe_cast(institution_type_code as string) institution_type_code,
    safe_cast(highest_degree_code as string) highest_degree_code,
    safe_cast(control_type_code as string) control_type_code,
    safe_cast(fy09_pilot_indicator as string) fy09_pilot_indicator
from {{ set_datalake_project("us_nsf_ncses_staging.herd_institution") }} as t
