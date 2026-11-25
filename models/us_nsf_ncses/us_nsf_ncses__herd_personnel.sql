{{
    config(
        schema="us_nsf_ncses",
        alias="herd_personnel",
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
    safe_cast(personnel_group as string) personnel_group,
    safe_cast(personnel_function as string) personnel_function,
    safe_cast(headcount as int64) headcount,
    safe_cast(headcount_status_code as string) headcount_status_code,
    safe_cast(full_time_equivalent as float64) full_time_equivalent,
    safe_cast(
        full_time_equivalent_status_code as string
    ) full_time_equivalent_status_code
from {{ set_datalake_project("us_nsf_ncses_staging.herd_personnel") }} as t
