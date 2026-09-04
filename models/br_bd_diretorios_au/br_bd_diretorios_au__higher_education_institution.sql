{{
    config(
        schema="br_bd_diretorios_au",
        alias="higher_education_institution",
        materialized="table",
    )
}}


select
    safe_cast(
        id_higher_education_institution as string
    ) id_higher_education_institution,
    safe_cast(name as string) name,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(provider_category as string) provider_category,
    safe_cast(is_aggregate as string) is_aggregate,
    safe_cast(provider_code as string) provider_code
from
    {{
        set_datalake_project(
            "br_bd_diretorios_au_staging.higher_education_institution"
        )
    }} as t
