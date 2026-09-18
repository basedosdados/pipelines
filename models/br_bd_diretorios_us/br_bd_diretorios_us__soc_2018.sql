{{
    config(
        alias="soc_2018",
        schema="br_bd_diretorios_us",
        materialized="table",
    )
}}
select
    safe_cast(id_soc as string) id_soc,
    safe_cast(name as string) name,
    safe_cast(level as string) level,
    safe_cast(id_major_group as string) id_major_group,
    safe_cast(name_major_group as string) name_major_group,
    safe_cast(id_minor_group as string) id_minor_group,
    safe_cast(name_minor_group as string) name_minor_group,
    safe_cast(id_broad_occupation as string) id_broad_occupation,
    safe_cast(name_broad_occupation as string) name_broad_occupation
from {{ set_datalake_project("br_bd_diretorios_us_staging.soc_2018") }} as t
