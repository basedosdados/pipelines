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
        cluster_by=["institution_id", "category"],
    )
}}


select
    safe_cast(year as int64) year,
    d.id_higher_education_institution institution_id,
    safe_cast(category as string) category,
    safe_cast(sub_category as string) sub_category,
    safe_cast(amount as int64) amount
from
    {{
        set_datalake_project(
            "au_doe_higher_education_finances_staging.research_income"
        )
    }} as t
left join
    {{ ref("br_bd_diretorios_au__higher_education_institution") }} as d
    on safe_cast(t.hep_code as string) = d.provider_code
