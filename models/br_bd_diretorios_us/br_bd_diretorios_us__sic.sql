{{
    config(
        alias="sic",
        schema="br_bd_diretorios_us",
        materialized="table",
    )
}}
select
    safe_cast(id_sic as string) id_sic,
    safe_cast(name as string) name,
    safe_cast(level as int64) level,
    safe_cast(id_industry_group as string) id_industry_group,
    safe_cast(name_industry_group as string) name_industry_group,
    safe_cast(id_major_group as string) id_major_group,
    safe_cast(name_major_group as string) name_major_group,
    safe_cast(id_division as string) id_division,
    safe_cast(name_division as string) name_division,
    safe_cast(name_sec as string) name_sec,
    safe_cast(id_sec_office as string) id_sec_office
from {{ set_datalake_project("br_bd_diretorios_us_staging.sic") }} as t
