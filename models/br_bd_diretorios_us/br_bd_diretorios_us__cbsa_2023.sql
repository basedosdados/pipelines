{{
    config(
        alias="cbsa_2023",
        schema="br_bd_diretorios_us",
        materialized="table",
    )
}}
select
    safe_cast(id_cbsa as string) id_cbsa,
    safe_cast(name as string) name,
    safe_cast(type as string) type,
    safe_cast(id_csa as string) id_csa,
    safe_cast(name_csa as string) name_csa
from {{ set_datalake_project("br_bd_diretorios_us_staging.cbsa_2023") }} as t
