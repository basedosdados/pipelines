{{
    config(
        schema="cn_hvd_cbdb",
        alias="address",
        materialized="table",
    )
}}


select
    safe_cast(person_id as string) person_id,
    safe_cast(address_code as string) address_code,
    safe_cast(address_type_code as string) address_type_code,
    safe_cast(sequence as string) sequence,
    safe_cast(first_year as int64) first_year,
    safe_cast(last_year as int64) last_year,
    safe_cast(is_natal as string) is_natal,
    safe_cast(source_id as string) source_id,
    safe_cast(source_pages as string) source_pages,
    safe_cast(notes as string) notes
from {{ set_datalake_project("cn_hvd_cbdb_staging.address") }} as t
