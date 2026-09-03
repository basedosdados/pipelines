{{
    config(
        alias="higher_education_provider",
        schema="br_bd_diretorios_au",
        materialized="table",
    )
}}
select
    safe_cast(hep_code as string) hep_code,
    safe_cast(name as string) name,
    safe_cast(abbreviation_state as string) abbreviation_state,
    safe_cast(cohort as string) cohort
from
    {{ set_datalake_project("br_bd_diretorios_au_staging.higher_education_provider") }}
    as t
