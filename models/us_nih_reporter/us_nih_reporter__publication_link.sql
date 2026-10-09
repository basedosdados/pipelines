{{
    config(
        schema="us_nih_reporter",
        alias="publication_link",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1980, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(pmid as string) pmid,
    safe_cast(core_project_num as string) core_project_num
from {{ set_datalake_project("us_nih_reporter_staging.publication_link") }} as t
