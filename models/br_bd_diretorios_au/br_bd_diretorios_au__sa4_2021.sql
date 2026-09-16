{{
    config(
        alias="sa4_2021",
        schema="br_bd_diretorios_au",
        materialized="table",
    )
}}
select
    safe_cast(id_sa4 as string) id_sa4,
    safe_cast(name as string) name,
    safe_cast(id_gccsa as string) id_gccsa,
    safe_cast(name_gccsa as string) name_gccsa,
    safe_cast(id_state as string) id_state,
    safe_cast(abbreviation_state as string) abbreviation_state,
    safe_cast(name_state as string) name_state,
    safe_cast(area_albers_sqkm as float64) area_albers_sqkm
from {{ set_datalake_project("br_bd_diretorios_au_staging.sa4_2021") }} as t
