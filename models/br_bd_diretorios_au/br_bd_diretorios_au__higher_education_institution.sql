{{
    config(
        schema="br_bd_diretorios_au",
        alias="higher_education_institution",
        materialized="table",
    )
}}


-- ``cohort`` is the provider's peak-body alignment (Go8, IRU, RUN, ATN), which
-- only the HERDC provider list carries. It is joined in from that staging table
-- rather than kept in a second directory: the two directories held the same 43
-- providers under two keys, and this one is the superset.
with
    institution as (
        select *
        from
            {{
                set_datalake_project(
                    "br_bd_diretorios_au_staging.higher_education_institution"
                )
            }}
    ),
    provider as (
        select
            safe_cast(hep_code as string) hep_code, safe_cast(cohort as string) cohort
        from
            {{
                set_datalake_project(
                    "br_bd_diretorios_au_staging.higher_education_provider"
                )
            }}
    )
select
    safe_cast(
        i.id_higher_education_institution as string
    ) id_higher_education_institution,
    safe_cast(i.name as string) name,
    safe_cast(i.state_abbreviation as string) state_abbreviation,
    safe_cast(i.provider_category as string) provider_category,
    safe_cast(i.is_aggregate as string) is_aggregate,
    safe_cast(i.provider_code as string) provider_code,
    p.cohort cohort
from institution as i
left join provider as p on safe_cast(i.provider_code as string) = p.hep_code
