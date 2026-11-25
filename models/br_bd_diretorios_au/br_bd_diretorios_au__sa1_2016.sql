{{
    config(
        alias="sa1_2016",
        schema="br_bd_diretorios_au",
        materialized="table",
    )
}}
select
    safe_cast(id_sa1 as string) id_sa1,
    safe_cast(id_sa1_short as string) id_sa1_short,
    safe_cast(id_sa2 as string) id_sa2,
    safe_cast(name_sa2 as string) name_sa2,
    safe_cast(id_sa3 as string) id_sa3,
    safe_cast(name_sa3 as string) name_sa3,
    safe_cast(id_sa4 as string) id_sa4,
    safe_cast(name_sa4 as string) name_sa4,
    safe_cast(id_gccsa as string) id_gccsa,
    safe_cast(name_gccsa as string) name_gccsa,
    safe_cast(id_state as string) id_state,
    safe_cast(abbreviation_state as string) abbreviation_state,
    safe_cast(name_state as string) name_state,
    safe_cast(area_albers_sqkm as float64) area_albers_sqkm
from {{ set_datalake_project("br_bd_diretorios_au_staging.sa1_2016") }} as t
