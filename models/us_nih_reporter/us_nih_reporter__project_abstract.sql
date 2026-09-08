{{
    config(
        schema="us_nih_reporter",
        alias="project_abstract",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1985, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(application_id as string) application_id,
    safe_cast(abstract_text as string) abstract_text
from {{ set_datalake_project("us_nih_reporter_staging.project_abstract") }} as t
