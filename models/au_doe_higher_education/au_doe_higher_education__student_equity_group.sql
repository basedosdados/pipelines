{{
    config(
        schema="au_doe_higher_education",
        alias="student_equity_group",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2011, "end": 2029, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(institution_id as string) institution_id,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(student_group as string) student_group,
    safe_cast(equity_group as string) equity_group,
    safe_cast(equity_group_classification as string) equity_group_classification,
    safe_cast(address_basis as string) address_basis,
    safe_cast(equity_group_label as string) equity_group_label,
    safe_cast(students as int64) students
from
    {{ set_datalake_project("au_doe_higher_education_staging.student_equity_group") }}
    as t
