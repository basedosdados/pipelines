{{
    config(
        schema="us_nsf_ncses",
        alias="herd_survey_item",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2010, "end": 2029, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(institution_id as string) institution_id,
    safe_cast(unitid as string) unitid,
    safe_cast(survey_form as string) survey_form,
    safe_cast(question_code as string) question_code,
    safe_cast(question as string) question,
    safe_cast(row_label as string) row_label,
    safe_cast(column_label as string) column_label,
    safe_cast(response_code as string) response_code,
    safe_cast(amount as float64) amount,
    safe_cast(other_information as string) other_information
from {{ set_datalake_project("us_nsf_ncses_staging.herd_survey_item") }} as t
