{{
    config(
        schema="cn_hvd_cbdb",
        alias="association_code",
        materialized="table",
    )
}}


select
    safe_cast(association_code as string) association_code,
    safe_cast(description_english as string) description_english,
    safe_cast(description_chinese as string) description_chinese,
    safe_cast(role_type as string) role_type,
    safe_cast(reciprocal_code as string) reciprocal_code,
    safe_cast(example as string) example
from {{ set_datalake_project("cn_hvd_cbdb_staging.association_code") }} as t
