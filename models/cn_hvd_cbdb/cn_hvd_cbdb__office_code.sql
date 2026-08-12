{{
    config(
        schema="cn_hvd_cbdb",
        alias="office_code",
        materialized="table",
    )
}}


select
    safe_cast(office_code as string) office_code,
    safe_cast(dynasty_code as string) dynasty_code,
    safe_cast(name_pinyin as string) name_pinyin,
    safe_cast(name_chinese as string) name_chinese,
    safe_cast(name_pinyin_alt as string) name_pinyin_alt,
    safe_cast(name_chinese_alt as string) name_chinese_alt,
    safe_cast(name_english as string) name_english,
    safe_cast(name_english_alt as string) name_english_alt,
    safe_cast(notes as string) notes
from {{ set_datalake_project("cn_hvd_cbdb_staging.office_code") }} as t
