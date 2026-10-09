{{
    config(
        schema="us_harvard_cbdb",
        alias="office_posting",
        materialized="table",
    )
}}


select
    safe_cast(person_id as string) person_id,
    safe_cast(office_code as string) office_code,
    safe_cast(posting_id as string) posting_id,
    safe_cast(sequence as string) sequence,
    safe_cast(first_year as int64) first_year,
    safe_cast(last_year as int64) last_year,
    safe_cast(appointment_code as string) appointment_code,
    safe_cast(assume_office_code as string) assume_office_code,
    safe_cast(dynasty_code as string) dynasty_code,
    safe_cast(source_id as string) source_id,
    safe_cast(source_pages as string) source_pages,
    safe_cast(notes as string) notes
from {{ set_datalake_project("us_harvard_cbdb_staging.office_posting") }} as t
