{{
    config(
        schema="us_harvard_cbdb",
        alias="address_code",
        materialized="table",
    )
}}


select
    safe_cast(address_code as string) address_code,
    safe_cast(name_pinyin as string) name_pinyin,
    safe_cast(name_chinese as string) name_chinese,
    safe_cast(first_year as int64) first_year,
    safe_cast(last_year as int64) last_year,
    safe_cast(admin_type as string) admin_type,
    safe_cast(admin_category_code as string) admin_category_code,
    safe_cast(longitude as float64) longitude,
    safe_cast(latitude as float64) latitude,
    safe_cast(chgis_point_id as string) chgis_point_id,
    safe_cast(alt_names as string) alt_names,
    safe_cast(notes as string) notes
from {{ set_datalake_project("us_harvard_cbdb_staging.address_code") }} as t
