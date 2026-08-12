{{
    config(
        schema="cn_hvd_cbdb",
        alias="kinship_code",
        materialized="table",
    )
}}


select
    safe_cast(kinship_code as string) kinship_code,
    safe_cast(relation_chinese as string) relation_chinese,
    safe_cast(relation_english as string) relation_english,
    safe_cast(relation_english_alt as string) relation_english_alt,
    safe_cast(relation_simplified as string) relation_simplified,
    safe_cast(reciprocal_code_1 as string) reciprocal_code_1,
    safe_cast(reciprocal_code_2 as string) reciprocal_code_2,
    safe_cast(generations_up as int64) generations_up,
    safe_cast(generations_down as int64) generations_down,
    safe_cast(marriage_steps as int64) marriage_steps,
    safe_cast(collateral_steps as int64) collateral_steps
from {{ set_datalake_project("cn_hvd_cbdb_staging.kinship_code") }} as t
