{{
    config(
        schema="cn_hvd_cbdb",
        alias="person",
        materialized="table",
    )
}}


select
    safe_cast(person_id as string) person_id,
    safe_cast(name_pinyin as string) name_pinyin,
    safe_cast(name_chinese as string) name_chinese,
    safe_cast(surname_pinyin as string) surname_pinyin,
    safe_cast(surname_chinese as string) surname_chinese,
    safe_cast(given_name_pinyin as string) given_name_pinyin,
    safe_cast(given_name_chinese as string) given_name_chinese,
    safe_cast(sex as string) sex,
    safe_cast(dynasty_code as string) dynasty_code,
    safe_cast(birth_year as int64) birth_year,
    safe_cast(death_year as int64) death_year,
    safe_cast(death_age as int64) death_age,
    safe_cast(index_year as int64) index_year,
    safe_cast(index_year_type_code as string) index_year_type_code,
    safe_cast(flourished_earliest_year as int64) flourished_earliest_year,
    safe_cast(flourished_latest_year as int64) flourished_latest_year,
    safe_cast(ethnicity_code as string) ethnicity_code,
    safe_cast(household_status_code as string) household_status_code,
    safe_cast(choronym_code as string) choronym_code,
    safe_cast(index_address_code as string) index_address_code,
    safe_cast(notes as string) notes
from {{ set_datalake_project("cn_hvd_cbdb_staging.person") }} as t
