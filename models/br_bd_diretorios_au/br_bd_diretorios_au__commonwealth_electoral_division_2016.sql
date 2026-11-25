{{
    config(
        alias="commonwealth_electoral_division_2016",
        schema="br_bd_diretorios_au",
        materialized="table",
    )
}}
select
    safe_cast(
        id_commonwealth_electoral_division as string
    ) id_commonwealth_electoral_division,
    safe_cast(name as string) name,
    safe_cast(id_state as string) id_state,
    safe_cast(abbreviation_state as string) abbreviation_state,
    safe_cast(name_state as string) name_state,
    safe_cast(area_albers_sqkm as float64) area_albers_sqkm
from
    {{
        set_datalake_project(
            "br_bd_diretorios_au_staging.commonwealth_electoral_division_2016"
        )
    }} as t
