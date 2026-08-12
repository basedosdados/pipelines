{{
    config(
        schema="us_harvard_cbdb",
        alias="kinship",
        materialized="table",
    )
}}


select
    safe_cast(person_id as string) person_id,
    safe_cast(kin_person_id as string) kin_person_id,
    safe_cast(kinship_code as string) kinship_code,
    safe_cast(source_id as string) source_id,
    safe_cast(source_pages as string) source_pages,
    safe_cast(notes as string) notes
from {{ set_datalake_project("us_harvard_cbdb_staging.kinship") }} as t
