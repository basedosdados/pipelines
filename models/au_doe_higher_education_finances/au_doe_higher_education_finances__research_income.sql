{{
    config(
        schema="au_doe_higher_education_finances",
        alias="research_income",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1992, "end": 2031, "interval": 1},
        },
        cluster_by=["hep_code", "category"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(hep_code as string) hep_code,
    safe_cast(category as string) category,
    safe_cast(sub_category as string) sub_category,
    safe_cast(amount as int64) amount
from
    {{
        set_datalake_project(
            "au_doe_higher_education_finances_staging.research_income"
        )
    }} as t
