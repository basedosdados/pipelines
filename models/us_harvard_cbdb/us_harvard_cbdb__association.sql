{{
    config(
        schema="us_harvard_cbdb",
        alias="association",
        materialized="table",
    )
}}


select
    safe_cast(person_id as string) person_id,
    safe_cast(assoc_person_id as string) assoc_person_id,
    safe_cast(association_code as string) association_code,
    safe_cast(kinship_code as string) kinship_code,
    safe_cast(kin_person_id as string) kin_person_id,
    safe_cast(assoc_kinship_code as string) assoc_kinship_code,
    safe_cast(assoc_kin_person_id as string) assoc_kin_person_id,
    safe_cast(first_year as int64) first_year,
    safe_cast(last_year as int64) last_year,
    safe_cast(address_code as string) address_code,
    safe_cast(text_title as string) text_title,
    safe_cast(source_id as string) source_id,
    safe_cast(source_pages as string) source_pages,
    safe_cast(notes as string) notes
from {{ set_datalake_project("us_harvard_cbdb_staging.association") }} as t
