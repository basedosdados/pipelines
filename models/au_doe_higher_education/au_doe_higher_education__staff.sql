{{
    config(
        schema="au_doe_higher_education",
        alias="staff",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2018, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(institution_id as string) institution_id,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(gender as string) gender,
    safe_cast(duties_classification as string) duties_classification,
    safe_cast(function as string) function,
    safe_cast(organisational_unit as string) organisational_unit,
    safe_cast(work_contract as string) work_contract,
    safe_cast(staff_headcount as int64) staff_headcount,
    safe_cast(staff_fte as float64) staff_fte
from {{ set_datalake_project("au_doe_higher_education_staging.staff") }} as t
